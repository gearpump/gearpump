/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package akka.stream.gearpump

import java.{util => ju}

import _root_.io.gearpump.util.{Graph => GGraph}
import akka.actor.ActorSystem
import akka.stream.Attributes.Attribute
import akka.stream._
import akka.stream.gearpump.GearpumpMaterializer.{Edge, MaterializedValueSourceAttribute}
import akka.stream.gearpump.graph.GraphCutter.Strategy
import akka.stream.gearpump.graph.LocalGraph.LocalGraphMaterializer
import akka.stream.gearpump.graph.RemoteGraph.RemoteGraphMaterializer
import akka.stream.gearpump.graph.{GraphCutter, LocalGraph, RemoteGraph, SubGraphMaterializer}
import akka.stream.impl.StreamLayout._
import akka.stream.impl._
import akka.stream.impl.fusing.GraphStageModule
import akka.stream.impl.fusing.GraphStages.MaterializedValueSource

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.FiniteDuration
import scala.collection.JavaConverters._

/**
 *
 * [[GearpumpMaterializer]] allows you to render akka-stream DSL as a Gearpump
 * streaming application. If some module cannot be rendered remotely in Gearpump
 * Cluster, then it will use local Actor materializer as fallback to materialize
 * the module locally.
 *
 * User can custom a [[Strategy]] to determinie which module should be rendered
 * remotely, and which module should be rendered locally.
 *
 * @see [[GraphCutter]] to find out how we cut the [[ModuleGraph]] to two parts,
 *   and materialize them seperately.
 * @param system ActorSystem
 * @param strategy Strategy
 * @param useLocalCluster whether to use built-in in-process local cluster
 */
class GearpumpMaterializer(system: ActorSystem, strategy: Strategy = GraphCutter.AllRemoteStrategy, useLocalCluster: Boolean = true, namePrefix: Option[String] = None)
    extends akka.stream.Materializer {


  private[this] def createFlowName(): String = FlowNames(system).name.copy( namePrefix.getOrElse("flow")).next()

  private val subMaterializers: Map[Class[_], SubGraphMaterializer] = Map(
    classOf[LocalGraph] -> new LocalGraphMaterializer(system),
    classOf[RemoteGraph] -> new RemoteGraphMaterializer(useLocalCluster, system)
  )

  override def withNamePrefix(name: String): Materializer = throw new UnsupportedOperationException()

  override implicit def executionContext: ExecutionContextExecutor = throw new UnsupportedOperationException()

  override def schedulePeriodically(initialDelay: FiniteDuration, interval: FiniteDuration, task: Runnable) =
    system.scheduler.schedule(initialDelay, interval, task)(executionContext)

  override def scheduleOnce(delay: FiniteDuration, task: Runnable) =
    system.scheduler.scheduleOnce(delay, task)(executionContext)

  override def materialize[Mat](runnableGraph: Graph[ClosedShape, Mat]): Mat = {
    val initialAttributes = Attributes()

    val session = new MaterializerSession(runnableGraph.module, initialAttributes) {
      private val flowName = createFlowName()
      private var nextId = 0
      private def stageName(attr: Attributes): String = {
        val name = s"$flowName-$nextId-${attr.nameOrDefault()}"
        nextId += 1
        name
      }
      val graph = GGraph.empty[Module, Edge]

      protected var subscribersStack: List[mutable.Map[InPort, (InPort, Module)]] =
        mutable.Map.empty[InPort, (InPort, Module)].withDefaultValue(null) :: Nil
      protected var publishersStack: List[mutable.Map[OutPort, (OutPort, Module)]] =
        mutable.Map.empty[OutPort, (OutPort, Module)].withDefaultValue(null) :: Nil

      protected var moduleStack: List[Module] = topLevel :: Nil
      protected def subscribers: mutable.Map[InPort, (InPort, Module)] = subscribersStack.head
      protected def publishers: mutable.Map[OutPort, (OutPort, Module)] = publishersStack.head
      protected def currentLayout: Module = moduleStack.head

      private def addEdge(publisher: (OutPort, Module), subscriber: (InPort, Module)): Unit = {
        graph.addEdge(publisher._2, Edge(publisher._1, subscriber._1), subscriber._2)
      }

      private def addVertex(module: Module): Unit = {
        graph.addVertex(module)
      }

      protected def assignPort(in: InPort, subscriber: (InPort, Module)): Unit = {
        addVertex(subscriber._2)
        subscribers(in) = subscriber
        // Interface (unconnected) ports of the current scope will be wired when exiting the scope
        if (!currentLayout.inPorts(in)) {
          val out = currentLayout.upstreams(in)
          val publisher = publishers(out)
          if (publisher ne null) addEdge(publisher, subscriber)
        }
      }

      protected def assignPort(out: OutPort, publisher: (OutPort, Module)): Unit = {
        addVertex(publisher._2)
        publishers(out) = publisher
        // Interface (unconnected) ports of the current scope will be wired when exiting the scope
        if (!currentLayout.outPorts(out)) {
          val in = currentLayout.downstreams(out)
          val subscriber = subscribers(in)
          if (subscriber ne null) addEdge(publisher, subscriber)
        }
      }

      // Enters a copied module and establishes a scope that prevents internals to leak out and interfere with copies
      // of the same module.
      // We don't store the enclosing CopiedModule itself as state since we don't use it anywhere else than exit and enter
      protected def enterScope(enclosing: CopiedModule): Unit = {
        subscribersStack ::= mutable.Map.empty.withDefaultValue(null)
        publishersStack ::= mutable.Map.empty.withDefaultValue(null)
        moduleStack ::= enclosing.copyOf
      }

      // Exits the scope of the copied module and propagates Publishers/Subscribers to the enclosing scope assigning
      // them to the copied ports instead of the original ones (since there might be multiple copies of the same module
      // leading to port identity collisions)
      // We don't store the enclosing CopiedModule itself as state since we don't use it anywhere else than exit and enter
      protected def exitScope(enclosing: CopiedModule): Unit = {
        val scopeSubscribers = subscribers
        val scopePublishers = publishers
        subscribersStack = subscribersStack.tail
        publishersStack = publishersStack.tail
        moduleStack = moduleStack.tail

        // When we exit the scope of a copied module,  pick up the Subscribers/Publishers belonging to exposed ports of
        // the original module and assign them to the copy ports in the outer scope that we will return to
        enclosing.copyOf.shape.inlets.iterator.zip(enclosing.shape.inlets.iterator).foreach {
          case (original, exposed) => assignPort(exposed, scopeSubscribers(original))
        }

        enclosing.copyOf.shape.outlets.iterator.zip(enclosing.shape.outlets.iterator).foreach {
          case (original, exposed) => assignPort(exposed, scopePublishers(original))
        }
      }

      protected override def materializeModule(module: Module, parentAttributes: Attributes): Any = {

        val materializedValues = collection.mutable.HashMap.empty[Module, MaterializedValueNode]
        val currentAttributes = mergeAttributes(parentAttributes, module.attributes)

        var materializedValueSources = List.empty[MaterializedValueSource[_]]

        for (submodule <- module.subModules) {
          submodule match {
            case atomic if atomic.isAtomic =>
              materializeAtomic(atomic, currentAttributes, materializedValues.toMap[Module,Any].asJava)
            case copied: CopiedModule =>
              enterScope(copied)
              materializeModule(copied, currentAttributes)
              exitScope(copied)
            case composite =>
              materializedValues.put(composite, materializeComposite(composite, currentAttributes).asInstanceOf[MaterializedValueNode])
            case graphStageModule: GraphStageModule =>
              graphStageModule.stage match {
                case mv: MaterializedValueSource[_] =>
                  materializedValueSources :+= mv
                case _ =>
              }
          }
        }

        val mat = resolveMaterialized(module.materializedValueComputation, materializedValues)

        materializedValueSources.foreach{module =>
          val matAttribute = new MaterializedValueSourceAttribute(mat)
          val copied = copyAtomicModule(module.module, parentAttributes and Attributes(matAttribute))
          assignPort(module.shape.out, (copied.shape.outlets.head, copied))
          graph.addVertex(copied)
          materializedValues.put(copied, Atomic(copied))
        }
        mat

        val subGraphs = new GraphCutter(strategy).cut(graph)
        val matValues = subGraphs.foldLeft(Map.empty[Module, Any]){(map, subGraph) =>
          val materializer = subMaterializers(subGraph.getClass)
          map ++ materializer.materialize(subGraph, map)
        }
      }

      override protected def materializeComposite(composite: Module, effectiveAttributes: Attributes): Any = {
        materializeModule(composite, effectiveAttributes)
      }

      protected def materializeAtomic(atomic: Module, parentAttributes: Attributes, matVal: ju.Map[Module, Any]): Unit = {
        val (inputs, outputs) = (atomic.shape.inlets, atomic.shape.outlets)
        val copied = copyAtomicModule(atomic, parentAttributes)

        for ((in, id) <- inputs.zipWithIndex) {
          val inPort = inPortMapping(atomic, copied)(in)
          assignPort(in, (inPort, copied))
        }

        for ((out, id) <- outputs.zipWithIndex) {
          val outPort = outPortMapping(atomic, copied)(out)
          assignPort(out, (outPort, copied))
        }

        graph.addVertex(copied)
        Atomic(copied)
      }

      private def copyAtomicModule[T <: Module](module: T, parentAttributes: Attributes): T = {
        val currentAttributes = mergeAttributes(parentAttributes, module.attributes)
        module.withAttributes(currentAttributes).asInstanceOf[T]
      }

      private def outPortMapping(from: Module, to: Module): Map[OutPort, OutPort] = {
        from.shape.outlets.iterator.zip(to.shape.outlets.iterator).toList.toMap
      }

      private def inPortMapping(from: Module, to: Module): Map[InPort, InPort] = {
        from.shape.inlets.iterator.zip(to.shape.inlets.iterator).toList.toMap
      }

      protected def resolveMaterialized(matNode: MaterializedValueNode, materializedValues: collection.Map[Module, MaterializedValueNode]): MaterializedValueNode = matNode match {
        case Atomic(m)          => materializedValues(m)
        case Combine(f, d1, d2) => Combine(f, resolveMaterialized(d1, materializedValues), resolveMaterialized(d2, materializedValues))
        case Transform(f, d)    => Transform(f, resolveMaterialized(d, materializedValues))
        case Ignore             => Ignore
      }
    }
    session.materialize().asInstanceOf[Mat]

    /*
    val graph = ModuleGraph(runnableGraph)
    val subGraphs = new GraphCutter(strategy).cut(graph)
    val matValues = subGraphs.foldLeft(Map.empty[Module, Any]){(map, subGraph) =>
      val materializer = subMaterializers(subGraph.getClass)
      map ++ materializer.materialize(subGraph, map)
    }
    graph.resolve(matValues)
    */
  }

  def shutdown: Unit = {
    subMaterializers.values.foreach(_.shutdown)
  }
}

object GearpumpMaterializer{
  final case class Edge(from: OutPort, to: InPort)
  final case class MaterializedValueSourceAttribute(mat: MaterializedValueNode) extends Attribute

  def apply(system: ActorSystem) = new GearpumpMaterializer(system)
}