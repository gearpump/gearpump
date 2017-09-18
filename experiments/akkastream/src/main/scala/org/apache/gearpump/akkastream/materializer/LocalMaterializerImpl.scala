/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.akkastream.materializer

import java.util.concurrent.atomic.AtomicBoolean
import java.{util => ju}

import org.apache.gearpump.util.{Graph => GGraph}
import akka.actor.{ActorRef, ActorSystem, Cancellable, Deploy, PoisonPill}
import akka.dispatch.Dispatchers
import akka.event.{Logging, LoggingAdapter}
import akka.stream.impl.StreamLayout._
import akka.stream.impl._
import akka.stream.impl.fusing.GraphInterpreter.GraphAssembly
import akka.stream.impl.fusing.{ActorGraphInterpreter, Fold, GraphInterpreterShell, GraphModule, GraphStageModule}
import akka.stream.impl.fusing.GraphStages.MaterializedValueSource
import akka.stream.scaladsl.ModuleExtractor
import akka.stream.{ClosedShape, Graph => AkkaGraph, _}
import org.apache.gearpump.akkastream.GearpumpMaterializer.Edge
import org.apache.gearpump.akkastream.module.ReduceModule
import org.apache.gearpump.akkastream.util.MaterializedValueOps
import org.reactivestreams.{Publisher, Subscriber}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.FiniteDuration

/**
 * This materializer is functional equivalent to [[akka.stream.impl.ActorMaterializerImpl]]
 *
 * @param system System
 * @param settings ActorMaterializerSettings
 * @param dispatchers Dispatchers
 * @param supervisor ActorRef
 * @param haveShutDown AtomicBoolean
 * @param flowNames SeqActorName
 */
case class LocalMaterializerImpl (
    override val system: ActorSystem,
    override val settings: ActorMaterializerSettings,
    dispatchers: Dispatchers,
    override val supervisor: ActorRef,
    haveShutDown: AtomicBoolean,
    flowNames: SeqActorName)
  extends ExtendedActorMaterializer {

  override def logger: LoggingAdapter = Logging.getLogger(system, this)

  override def schedulePeriodically(initialDelay: FiniteDuration, interval: FiniteDuration,
      task: Runnable): Cancellable =
    system.scheduler.schedule(initialDelay, interval, task)(executionContext)

  override def scheduleOnce(delay: FiniteDuration, task: Runnable): Cancellable =
    system.scheduler.scheduleOnce(delay, task)(executionContext)

  override def effectiveSettings(opAttr: Attributes): ActorMaterializerSettings = {
    import ActorAttributes._
    import Attributes._
    opAttr.attributeList.foldLeft(settings) { (s, attr) =>
      attr match {
        case InputBuffer(initial, max) => s.withInputBuffer(initial, max)
        case Dispatcher(dispatcher) => s.withDispatcher(dispatcher)
        case SupervisionStrategy(decider) => s.withSupervisionStrategy(decider)
        case l: LogLevels => s
        case Name(_) => s
        case other => s
      }
    }
  }

  override def shutdown(): Unit =
    if (haveShutDown.compareAndSet(false, true)) supervisor ! PoisonPill

  override def isShutdown: Boolean = haveShutDown.get()

  override lazy val executionContext: ExecutionContextExecutor =
    dispatchers.lookup(settings.dispatcher match {
      case Deploy.NoDispatcherGiven => Dispatchers.DefaultDispatcherId
      case other => other
  })


  case class LocalMaterializerSession(module: Module, iAttributes: Attributes,
      subflowFuser: GraphInterpreterShell => ActorRef = null)
    extends MaterializerSession(module, iAttributes) {

    override def materializeAtomic(atomic: AtomicModule,
        effectiveAttributes: Attributes, matVal: ju.Map[Module, Any]): Unit = {

      def newMaterializationContext() =
        new MaterializationContext(LocalMaterializerImpl.this, effectiveAttributes,
          stageName(effectiveAttributes))
      atomic match {
        case sink: SinkModule[_, _] =>
          val (sub, mat) = sink.create(newMaterializationContext())
          assignPort(sink.shape.in, sub.asInstanceOf[Subscriber[Any]])
          matVal.put(atomic, mat)
        case source: SourceModule[_, _] =>
          val (pub, mat) = source.create(newMaterializationContext())
          assignPort(source.shape.out, pub.asInstanceOf[Publisher[Any]])
          matVal.put(atomic, mat)
        case stage: ProcessorModule[_, _, _] =>
          val (processor, mat) = stage.createProcessor()
          assignPort(stage.inPort, processor)
          assignPort(stage.outPort, processor.asInstanceOf[Publisher[Any]])
          matVal.put(atomic, mat)
        // FIXME
        //        case tls: TlsModule =>
        // TODO solve this so TlsModule doesn't need special treatment here
        //          val es = effectiveSettings(effectiveAttributes)
        //          val props =
        //            TLSActor.props(es, tls.sslContext, tls.sslConfig,
        //              tls.firstSession, tls.role, tls.closing, tls.hostInfo)
        //          val impl = actorOf(props, stageName(effectiveAttributes), es.dispatcher)
        //          def factory(id: Int) = new ActorPublisher[Any](impl) {
        //            override val wakeUpMsg = FanOut.SubstreamSubscribePending(id)
        //          }
        //          val publishers = Vector.tabulate(2)(factory)
        //          impl ! FanOut.ExposedPublishers(publishers)
        //
        //          assignPort(tls.plainOut, publishers(TLSActor.UserOut))
        //          assignPort(tls.cipherOut, publishers(TLSActor.TransportOut))
        //
        //          assignPort(tls.plainIn, FanIn.SubInput[Any](impl, TLSActor.UserIn))
        //          assignPort(tls.cipherIn, FanIn.SubInput[Any](impl, TLSActor.TransportIn))
        //
        //          matVal.put(atomic, NotUsed)
        case graph: GraphModule =>
          matGraph(graph, effectiveAttributes, matVal)
        case stage: GraphStageModule =>
          val graph =
            GraphModule(GraphAssembly(stage.shape.inlets, stage.shape.outlets, stage.stage),
              stage.shape, stage.attributes, Array(stage))
          matGraph(graph, effectiveAttributes, matVal)
      }
    }

    private def matGraph(graph: GraphModule, effectiveAttributes: Attributes,
        matVal: ju.Map[Module, Any]): Unit = {
      val calculatedSettings = effectiveSettings(effectiveAttributes)
      val (handlers, logics) =
        graph.assembly.materialize(effectiveAttributes, graph.matValIDs, matVal, registerSrc)

      val shell = new GraphInterpreterShell(graph.assembly, handlers,
        logics, graph.shape, calculatedSettings, LocalMaterializerImpl.this)

      val impl =
        if (subflowFuser != null && !effectiveAttributes.contains(Attributes.AsyncBoundary)) {
          subflowFuser(shell)
        } else {
          val props = ActorGraphInterpreter.props(shell)
          actorOf(props, stageName(effectiveAttributes), calculatedSettings.dispatcher)
        }

      for ((inlet, i) <- graph.shape.inlets.iterator.zipWithIndex) {
        val subscriber = new ActorGraphInterpreter.BoundarySubscriber(impl, shell, i)
        assignPort(inlet, subscriber)
      }
      for ((outlet, i) <- graph.shape.outlets.iterator.zipWithIndex) {
        val publisher = new ActorGraphInterpreter.BoundaryPublisher(impl, shell, i)
        impl ! ActorGraphInterpreter.ExposedPublisher(shell, i, publisher)
        assignPort(outlet, publisher)
      }
    }
  }

  override def materialize[Mat](runnableGraph: AkkaGraph[ClosedShape, Mat]): Mat = {

    LocalMaterializerSession(ModuleExtractor.unapply(runnableGraph).get,
      null, null).materialize().asInstanceOf[Mat]

  }

  override def materialize[Mat](runnableGraph: AkkaGraph[ClosedShape, Mat],
      initialAttributes: Attributes): Mat = {
    materialize(runnableGraph)
  }

  override def materialize[Mat](runnableGraph: AkkaGraph[ClosedShape, Mat],
      subflowFuser: GraphInterpreterShell => ActorRef): Mat = {

    LocalMaterializerSession(ModuleExtractor.unapply(runnableGraph).get,
      null, null).materialize().asInstanceOf[Mat]

  }

  override def materialize[Mat](runnableGraph: AkkaGraph[ClosedShape, Mat],
      subflowFuser: (GraphInterpreterShell) => ActorRef, initialAttributes: Attributes): Mat = {
    materialize(runnableGraph)
  }

  override def makeLogger(logSource: Class[_]): LoggingAdapter = {
    logger
  }

  def buildToplevelModule(graph: GGraph[Module, Edge]): Module = {
    var moduleInProgress: Module = EmptyModule
    graph.getVertices.foreach(module => {
      moduleInProgress = moduleInProgress.compose(module)
    })
    graph.getEdges.foreach(value => {
      val (node1, edge, node2) = value
      moduleInProgress = moduleInProgress.wire(edge.from, edge.to)
    })

    moduleInProgress
  }

  def materialize(graph: GGraph[Module, Edge],
      inputMatValues: scala.collection.mutable.Map[Module, Any]):
      scala.collection.mutable.Map[Module, Any] = {
    val topLevelModule = buildToplevelModule(graph)
    val session = LocalMaterializerSession(topLevelModule, null, null)
    import scala.collection.JavaConverters._
    val matV = inputMatValues.asJava
    val materializedGraph = graph.mapVertex { module =>
      session.materializeAtomic(module.asInstanceOf[AtomicModule], module.attributes, matV)
      matV.get(module)
    }
    materializedGraph.getEdges.foreach { nodeEdgeNode =>
      val (node1, edge, node2) = nodeEdgeNode
      val from = edge.from
      val to = edge.to
      node1 match {
        case module1: Module =>
          node2 match {
            case module2: Module =>
              val publisher = module1.downstreams(from).asInstanceOf[Publisher[Any]]
              val subscriber = module2.upstreams(to).asInstanceOf[Subscriber[Any]]
              publisher.subscribe(subscriber)
            case _ =>
          }
        case _ =>
      }
    }
    val matValSources = graph.getVertices.flatMap(module => {
      val rt: Option[MaterializedValueSource[_]] = module match {
        case graphStage: GraphStageModule =>
          graphStage.stage match {
            case materializedValueSource: MaterializedValueSource[_] =>
              Some(materializedValueSource)
            case _ =>
              None
          }
        case _ =>
          None
      }
      rt
    })
    publishToMaterializedValueSource(matValSources, inputMatValues)
    inputMatValues
  }

  private def publishToMaterializedValueSource(modules: List[MaterializedValueSource[_]],
      matValues: scala.collection.mutable.Map[Module, Any]): Unit = {
    modules.foreach { source =>
      Option(source.computation).map { attr =>
        val valueToPublish = MaterializedValueOps(attr).resolve(matValues)
        source.setValue(valueToPublish)
      }
    }
  }

  private[this] def createFlowName(): String = flowNames.next()

  val flowName = createFlowName()
  var nextId = 0

  def stageName(attr: Attributes): String = {
    val name = s"$flowName-$nextId-${attr.nameOrDefault()}"
    nextId += 1
    name
  }

  override def withNamePrefix(name: String): LocalMaterializerImpl =
    this.copy(flowNames = flowNames.copy(name))

}

object LocalMaterializerImpl {
  case class MaterializedModule(module: Module, matValue: Any,
      inputs: Map[InPort, Subscriber[_]] = Map.empty[InPort, Subscriber[_]],
      outputs: Map[OutPort, Publisher[_]] = Map.empty[OutPort, Publisher[_]])

  def apply(materializerSettings: Option[ActorMaterializerSettings] = None,
      namePrefix: Option[String] = None)(implicit system: ActorSystem):
  LocalMaterializerImpl = {

    val settings = materializerSettings getOrElse ActorMaterializerSettings(system)
    apply(settings, namePrefix.getOrElse("flow"))(system)
  }

  def apply(materializerSettings: ActorMaterializerSettings,
      namePrefix: String)(implicit system: ActorSystem): LocalMaterializerImpl = {
    val haveShutDown = new AtomicBoolean(false)

    new LocalMaterializerImpl(
      system,
      materializerSettings,
      system.dispatchers,
      system.actorOf(StreamSupervisor.props(materializerSettings,
        haveShutDown).withDispatcher(materializerSettings.dispatcher)),
      haveShutDown,
      FlowNames(system).name.copy(namePrefix))
  }

  def toFoldModule(reduce: ReduceModule[Any]): Fold[Any, Any] = {
    val f = reduce.f
    val aggregator = {(zero: Any, input: Any) =>
      if (zero == null) {
        input
      } else {
        f(zero, input)
      }
    }
    Fold(null, aggregator)
  }
}
