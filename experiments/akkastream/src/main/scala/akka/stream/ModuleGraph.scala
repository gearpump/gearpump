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

package akka.stream

import _root_.io.gearpump.util
import _root_.io.gearpump.util.Graph
import akka.stream.ModuleGraph.Edge
import akka.stream.gearpump.module.BridgeModule
import akka.stream.impl.GenJunctions.{UnzipWithModule, ZipWithModule}
import akka.stream.impl.Junctions.{BalanceModule, BroadcastModule, ConcatModule, FanInModule, FanOutModule, FlexiMergeModule, FlexiRouteModule, JunctionModule, MergeModule, MergePreferredModule}
import akka.stream.impl.Stages.StageModule
import akka.stream.impl.StreamLayout._
import akka.stream.impl._
import akka.stream.io.SslTls.TlsModule
import akka.stream.{Graph => AkkaGraph}

import scala.collection.mutable

class ModuleGraph[Mat](val graph: util.Graph[Module, Edge], val mat: MaterializedValueNode) {

  def resolve(materializedValues: Map[Module, Any]): Mat = {
    def resolveMaterialized(mat: MaterializedValueNode, materializedValues: Map[Module, Any]): Any = mat match {
      case Atomic(m) => materializedValues.getOrElse(m, ())
      case Combine(f, d1, d2) => f(resolveMaterialized(d1, materializedValues), resolveMaterialized(d2, materializedValues))
      case Transform(f, d)    => f(resolveMaterialized(d, materializedValues))
      case Ignore             => ()
    }
    resolveMaterialized(mat, materializedValues).asInstanceOf[Mat]
  }
}

object ModuleGraph {

  def apply[Mat](runnableGraph: AkkaGraph[ClosedShape, Mat]): ModuleGraph[Mat] = {
    val topLevel = runnableGraph.module
    val factory = new ModuleGraphFactory(topLevel)
    val (graph, mat) =  factory.create()
    new ModuleGraph(graph, mat)
  }

  case class Edge(from: OutPort, to: InPort)

  private class ModuleGraphFactory(val topLevel: StreamLayout.Module) {

    private var subscribersStack: List[mutable.Map[InPort, (InPort, Module)]] =
      mutable.Map.empty[InPort, (InPort, Module)].withDefaultValue(null) :: Nil
    private var publishersStack: List[mutable.Map[OutPort, (OutPort, Module)]] =
      mutable.Map.empty[OutPort, (OutPort, Module)].withDefaultValue(null) :: Nil

    /*
     * Please note that this stack keeps track of the scoped modules wrapped in CopiedModule but not the CopiedModule
     * itself. The reason is that the CopiedModule itself is only needed for the enterScope and exitScope methods but
     * not elsewhere. For this reason they are just simply passed as parameters to those methods.
     *
     * The reason why the encapsulated (copied) modules are stored as mutable state to save subclasses of this class
     * from passing the current scope around or even knowing about it.
     */
    private var moduleStack: List[Module] = topLevel :: Nil

    private def subscribers: mutable.Map[InPort, (InPort, Module)] = subscribersStack.head
    private def publishers: mutable.Map[OutPort, (OutPort, Module)] = publishersStack.head
    private def currentLayout: Module = moduleStack.head

    private val graph = Graph.empty[Module, Edge]

    private def copyAtomicModule[T <: Module](module: T, parentAttributes: Attributes): T = {
      val currentAttributes = mergeAttributes(parentAttributes, module.attributes)
      module.withAttributes(currentAttributes).asInstanceOf[T]
    }

    private def materializeAtomic(atomic: Module, parentAttributes: Attributes): MaterializedValueNode = {
      val copied = atomic match {
        case bridge: BridgeModule[_, _, _] =>
          val copied = copyAtomicModule(bridge, parentAttributes)
          assignPort(bridge.inPort, (copied.inPort, copied))
          assignPort(bridge.outPort, (copied.outPort, copied))
          copied
        case sink: SinkModule[_, _] =>
          val inlet = sink.shape.inlet
          val copied = copyAtomicModule(sink, parentAttributes)
          assignPort(inlet, (copied.shape.inlet, copied))
          copied
        case source: SourceModule[_, _] =>
          val outlet = source.shape.outlet
          val copied = copyAtomicModule(source, parentAttributes)
          assignPort(outlet, (copied.shape.outlet, copied))
          copied
        case stage: StageModule =>
          val copied = copyAtomicModule(stage, parentAttributes)
          assignPort(stage.inPort, (copied.inPort, copied))
          assignPort(stage.outPort, (copied.outPort, copied))
          copied
        case tls: TlsModule =>
          val copied = copyAtomicModule(tls, parentAttributes)
          assignPort(tls.plainOut, (copied.plainOut, copied))
          assignPort(tls.cipherOut, (copied.cipherOut, copied))
          assignPort(tls.plainIn, (copied.plainIn, copied))
          assignPort(tls.cipherIn, (copied.cipherIn, copied))
          copied
        case junction: JunctionModule =>
          materializeJunction(junction, parentAttributes)
      }
      graph.addVertex(copied)
      Atomic(copied)
    }

    private def materializeJunction(op: JunctionModule,
       parentAttributes: Attributes): Module = {
      val copied = op match {
        case fanin: FanInModule =>
          val (inputs, output) = fanin match {
            case MergeModule(shape, _) =>
              (shape.inSeq, shape.out)

            case f: FlexiMergeModule[t, Shape] =>
              val flexi = f.flexi(f.shape)
              (f.shape.inlets, f.shape.outlets.head)

            case MergePreferredModule(shape, _) =>
              (shape.preferred +: shape.inSeq, shape.out)

            case ConcatModule(shape, _) =>
              (shape.inSeq, shape.out)

            case zip: ZipWithModule =>
              (zip.shape.inlets, zip.outPorts.head)
          }

          val copied = copyAtomicModule(fanin, parentAttributes)

          for ((in, id) <- inputs.zipWithIndex) {
            val inPort = inPortMapping(fanin, copied)(in)
            assignPort(in, (inPort, copied))
          }
          val outPort = outPortMapping(fanin, copied)(output)
          assignPort(output, (outPort, copied))
          copied

        case fanout: FanOutModule =>
          val (in, outs) = fanout match {

            case r: FlexiRouteModule[t, Shape] =>
              val flexi = r.flexi(r.shape)
              (r.shape.inlets.head: InPort, r.shape.outlets)

            case BroadcastModule(shape, eagerCancel, _) =>
              (shape.in, shape.outArray.toSeq)

            case BalanceModule(shape, waitForDownstreams, _) =>
              (shape.in, shape.outArray.toSeq)

            case unzip: UnzipWithModule =>
              (unzip.inPorts.head, unzip.shape.outlets)
          }

          val copied = copyAtomicModule(fanout, parentAttributes)

          publishers.iterator.zip(outs.iterator).foreach { case (pub, out) =>
            val outPort = outPortMapping(fanout, copied)(out)
            assignPort(out, (outPort, copied))
          }
          val inPort = inPortMapping(fanout, copied)(in)
          assignPort(in, (inPort, copied))
          copied
      }
      copied
    }

    def create(): (util.Graph[Module, Edge], MaterializedValueNode) = {
      val mat = materializeModule(topLevel, Attributes.none)
      (graph, mat)
    }

    private def outPortMapping(from: Module, to: Module): Map[OutPort, OutPort] = {
      from.shape.outlets.iterator.zip(to.shape.outlets.iterator).toList.toMap
    }

    private def inPortMapping(from: Module, to: Module): Map[InPort, InPort] = {
      from.shape.inlets.iterator.zip(to.shape.inlets.iterator).toList.toMap
    }

    private def materializeModule(module: Module, parentAttributes: Attributes): MaterializedValueNode = {

      val materializedValues = collection.mutable.HashMap.empty[Module, MaterializedValueNode]
      val currentAttributes = mergeAttributes(parentAttributes, module.attributes)

      for (submodule <- module.subModules) {
        submodule match {
          case mv: MaterializedValueSource[_] =>
            //TODO
            throw new UnsupportedOperationException()
          case atomic if atomic.isAtomic =>
            materializedValues.put(atomic, materializeAtomic(atomic, currentAttributes))
          case copied: CopiedModule =>
            enterScope(copied)
            materializedValues.put(copied, materializeModule(copied, currentAttributes))
            exitScope(copied)
          case composite =>
            materializedValues.put(composite, materializeComposite(composite, currentAttributes))
        }
      }

      val mat = resolveMaterialized(module.materializedValueComputation, materializedValues)
      mat
    }

    private def materializeComposite(composite: Module, effectiveAttributes: Attributes): MaterializedValueNode = {
      materializeModule(composite, effectiveAttributes)
    }

    private def mergeAttributes(parent: Attributes, current: Attributes): Attributes = {
      parent and current
    }

    private def resolveMaterialized(matNode: MaterializedValueNode, materializedValues: collection.Map[Module, MaterializedValueNode]): MaterializedValueNode = matNode match {
      case Atomic(m)          => materializedValues(m)
      case Combine(f, d1, d2) => Combine(f, resolveMaterialized(d1, materializedValues), resolveMaterialized(d2, materializedValues))
      case Transform(f, d)    => Transform(f, resolveMaterialized(d, materializedValues))
      case Ignore             => Ignore
    }

    final protected def assignPort(in: InPort, subscriber: (InPort, Module)): Unit = {
      addVertex(subscriber._2)
      subscribers(in) = subscriber
      // Interface (unconnected) ports of the current scope will be wired when exiting the scope
      if (!currentLayout.inPorts(in)) {
        val out = currentLayout.upstreams(in)
        val publisher = publishers(out)
        if (publisher ne null) addEdge(publisher, subscriber)
      }
    }

    final protected def assignPort(out: OutPort, publisher: (OutPort, Module)): Unit = {
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
    private def enterScope(enclosing: CopiedModule): Unit = {
      subscribersStack ::= mutable.Map.empty.withDefaultValue(null)
      publishersStack ::= mutable.Map.empty.withDefaultValue(null)
      moduleStack ::= enclosing.copyOf
    }

    // Exits the scope of the copied module and propagates Publishers/Subscribers to the enclosing scope assigning
    // them to the copied ports instead of the original ones (since there might be multiple copies of the same module
    // leading to port identity collisions)
    // We don't store the enclosing CopiedModule itself as state since we don't use it anywhere else than exit and enter
    private def exitScope(enclosing: CopiedModule): Unit = {
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

    private def addEdge(publisher: (OutPort, Module), subscriber: (InPort, Module)): Unit = {
      graph.addEdge(publisher._2, Edge(publisher._1, subscriber._1), subscriber._2)
    }

    private def addVertex(module: Module): Unit = {
      graph.addVertex(module)
    }
  }
}