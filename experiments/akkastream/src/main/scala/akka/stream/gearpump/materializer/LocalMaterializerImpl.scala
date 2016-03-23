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

package akka.stream.gearpump.materializer

import java.util.concurrent.atomic.AtomicBoolean
import java.{util => ju}

import _root_.io.gearpump.util.Graph
import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.Dispatchers
import akka.stream.actor.ActorSubscriber
import akka.stream.gearpump.GearpumpMaterializer.{Edge, MaterializedValueSourceAttribute}
import akka.stream.gearpump.materializer.LocalMaterializerImpl.MaterializedModule
import akka.stream.gearpump.module.ReduceModule
import akka.stream.gearpump.util.MaterializedValueOps
import akka.stream.impl.Stages.{DirectProcessor, StageModule}
import akka.stream.impl.StreamLayout.Module
import akka.stream.impl._
import akka.stream.impl.fusing.GraphInterpreter.GraphAssembly
import akka.stream.impl.fusing.GraphStages.MaterializedValueSource
import akka.stream.impl.fusing.{Map => MMap, _}
import akka.stream.impl.io.TlsModule
import akka.stream.scaladsl.{Balance, Broadcast, Concat}
import akka.stream.stage.GraphStage
import akka.stream.{ActorMaterializerSettings, Attributes, InPort, MaterializationContext, Materializer, OutPort, Shape, Graph => AkkaGraph}
import org.reactivestreams.{Processor, Publisher, Subscriber}

/**
 * This materializer is functional equivalent to [[akka.stream.impl.ActorMaterializerImpl]]
 *
 * @param system
 * @param settings
 * @param dispatchers
 * @param supervisor
 * @param haveShutDown
 * @param flowNames
 */
class LocalMaterializerImpl (
    system: ActorSystem,
    settings: ActorMaterializerSettings,
    dispatchers: Dispatchers,
    supervisor: ActorRef,
    haveShutDown: AtomicBoolean,
    flowNames: SeqActorName)
  extends LocalMaterializer(
    system, settings, dispatchers, supervisor,
    haveShutDown, flowNames){

  override def materialize(graph: Graph[Module, Edge], inputMatValues: Map[Module, Any]): Map[Module, Any] = {
    val materializedGraph = graph.mapVertex{module =>
      materializeAtomic(module)
    }

    materializedGraph.edges.foreach{nodeEdgeNode =>
      val (node1, edge, node2) = nodeEdgeNode
      val from = edge.from
      val to = edge.to
      val publisher = node1.outputs(from).asInstanceOf[Publisher[Any]]
      val subscriber = node2.inputs(to).asInstanceOf[Subscriber[Any]]
      publisher.subscribe(subscriber)
    }

    val matValues = inputMatValues ++ materializedGraph.vertices.map{vertex =>
      (vertex.module, vertex.matValue)
    }.toMap

    val matValueSources = materializedGraph.vertices.filter(_.module.isInstanceOf[MaterializedValueSource[_]])
    publishToMaterializedValueSource(matValueSources, matValues)

    matValues

    /*
        val session = new MaterializerSession(runnableGraph.module, initialAttributes) {
      private val flowName = createFlowName()
      private var nextId = 0
      private def stageName(attr: Attributes): String = {
        val name = s"$flowName-$nextId-${attr.nameOrDefault()}"
        nextId += 1
        name
      }

      override protected def materializeAtomic(atomic: Module, effectiveAttributes: Attributes, matVal: ju.Map[Module, Any]): Unit = {

        def newMaterializationContext() =
          new MaterializationContext(ActorMaterializerImpl.this, effectiveAttributes, stageName(effectiveAttributes))
        atomic match {
          case sink: SinkModule[_, _] ⇒
            val (sub, mat) = sink.create(newMaterializationContext())
            assignPort(sink.shape.in, sub.asInstanceOf[Subscriber[Any]])
            matVal.put(atomic, mat)
          case source: SourceModule[_, _] ⇒
            val (pub, mat) = source.create(newMaterializationContext())
            assignPort(source.shape.out, pub.asInstanceOf[Publisher[Any]])
            matVal.put(atomic, mat)

          // FIXME: Remove this, only stream-of-stream ops need it
          case stage: StageModule ⇒
            val (processor, mat) = processorFor(stage, effectiveAttributes, effectiveSettings(effectiveAttributes))
            assignPort(stage.inPort, processor)
            assignPort(stage.outPort, processor)
            matVal.put(atomic, mat)

          case tls: TlsModule ⇒ // TODO solve this so TlsModule doesn't need special treatment here
            val es = effectiveSettings(effectiveAttributes)
            val props =
              TLSActor.props(es, tls.sslContext, tls.firstSession, tls.role, tls.closing, tls.hostInfo)
            val impl = actorOf(props, stageName(effectiveAttributes), es.dispatcher)
            def factory(id: Int) = new ActorPublisher[Any](impl) {
              override val wakeUpMsg = FanOut.SubstreamSubscribePending(id)
            }
            val publishers = Vector.tabulate(2)(factory)
            impl ! FanOut.ExposedPublishers(publishers)

            assignPort(tls.plainOut, publishers(TLSActor.UserOut))
            assignPort(tls.cipherOut, publishers(TLSActor.TransportOut))

            assignPort(tls.plainIn, FanIn.SubInput[Any](impl, TLSActor.UserIn))
            assignPort(tls.cipherIn, FanIn.SubInput[Any](impl, TLSActor.TransportIn))

            matVal.put(atomic, NotUsed)

          case graph: GraphModule ⇒
            matGraph(graph, effectiveAttributes, matVal)

          case stage: GraphStageModule ⇒
            val graph =
              GraphModule(GraphAssembly(stage.shape.inlets, stage.shape.outlets, stage.stage),
                stage.shape, stage.attributes, Array(stage))
            matGraph(graph, effectiveAttributes, matVal)
        }
      }

      private def matGraph(graph: GraphModule, effectiveAttributes: Attributes, matVal: ju.Map[Module, Any]): Unit = {
        val calculatedSettings = effectiveSettings(effectiveAttributes)
        val (inHandlers, outHandlers, logics) = graph.assembly.materialize(effectiveAttributes, graph.matValIDs, matVal, registerSrc)

        val shell = new GraphInterpreterShell(graph.assembly, inHandlers, outHandlers, logics, graph.shape,
          calculatedSettings, ActorMaterializerImpl.this)

        val impl =
          if (subflowFuser != null && !effectiveAttributes.contains(Attributes.AsyncBoundary)) {
            subflowFuser(shell)
          } else {
            val props = ActorGraphInterpreter.props(shell)
            actorOf(props, stageName(effectiveAttributes), calculatedSettings.dispatcher)
          }

        for ((inlet, i) ← graph.shape.inlets.iterator.zipWithIndex) {
          val subscriber = new ActorGraphInterpreter.BoundarySubscriber(impl, shell, i)
          assignPort(inlet, subscriber)
        }
        for ((outlet, i) ← graph.shape.outlets.iterator.zipWithIndex) {
          val publisher = new ActorGraphInterpreter.BoundaryPublisher(impl, shell, i)
          impl ! ActorGraphInterpreter.ExposedPublisher(shell, i, publisher)
          assignPort(outlet, publisher)
        }
      }

      // FIXME: Remove this, only stream-of-stream ops need it
      private def processorFor(op: StageModule,
                               effectiveAttributes: Attributes,
                               effectiveSettings: ActorMaterializerSettings): (Processor[Any, Any], Any) = op match {
        case DirectProcessor(processorFactory, _) ⇒ processorFactory()
        case _ ⇒
          val (opprops, mat) = ActorProcessorFactory.props(ActorMaterializerImpl.this, op, effectiveAttributes)
          ActorProcessorFactory[Any, Any](
            actorOf(opprops, stageName(effectiveAttributes), effectiveSettings.dispatcher)) -> mat
      }
    }

     */
  }

  private def publishToMaterializedValueSource(modules: List[MaterializedModule], matValues: Map[Module, Any]) = {
    modules.foreach { module =>
      val source = module.module.asInstanceOf[MaterializedValueSource[_]]
      val attr = source.initialAttributes.getAttribute(classOf[MaterializedValueSourceAttribute], null)

      Option(attr).map { attr =>
        val valueToPublish = MaterializedValueOps(attr.mat).resolve[Any](matValues)
        module.outputs.foreach { portAndPublisher =>
          val (port, publisher) = portAndPublisher
          //TODO kkasravi review
          publisher match {
            case valuePublisher: ActorPublisher[_] =>
              valuePublisher.setValue(valueToPublish)
          }
        }
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

  private def materializeAtomic(atomic: Module): MaterializedModule = {
    val effectiveAttributes = atomic.attributes

    def newMaterializationContext() =
      new MaterializationContext(LocalMaterializerImpl.this, effectiveAttributes, stageName(effectiveAttributes))

    atomic match {
      case sink: SinkModule[_, _] =>
        val (sub, mat) = sink.create(newMaterializationContext())
        val inputs = Map[InPort, Subscriber[_]](sink.shape.in -> sub)
        MaterializedModule(sink, mat, inputs)
      case source: SourceModule[_, _] =>
        val (pub, mat) = source.create(newMaterializationContext())
        val outputs = Map[OutPort, Publisher[_]](source.shape.out -> pub)
        MaterializedModule(source, mat, outputs = outputs)
      case reduce: ReduceModule[Any] =>
      //TODO: remove this after the official akka-stream API support the Reduce Module
        val stage  = LocalMaterializerImpl.toFoldModule(reduce)//.create(effectiveAttributes)
        val (processor, mat) = processorFor(stage, effectiveAttributes, effectiveSettings(effectiveAttributes))
        val inputs = Map[InPort, Subscriber[_]](stage.inPort -> processor)
        val outputs = Map[OutPort, Publisher[_]](stage.outPort -> processor)
        MaterializedModule(stage, mat, inputs, outputs)

      case stage: StageModule =>
        val (processor, mat) = processorFor(stage, effectiveAttributes, effectiveSettings(effectiveAttributes))
        val inputs = Map[InPort, Subscriber[_]](stage.inPort -> processor)
        val outputs = Map[OutPort, Publisher[_]](stage.outPort -> processor)
        MaterializedModule(stage, mat, inputs, outputs)
      case tls: TlsModule => // TODO solve this so TlsModule doesn't need special treatment here
        val es = effectiveSettings(effectiveAttributes)
        val props =
          SslTlsCipherActor.props(es, tls.sslContext, tls.firstSession, tls.role, tls.closing, tls.hostInfo, tracing = false)
        val impl = actorOf(props, stageName(effectiveAttributes), es.dispatcher)
        def factory(id: Int) = new ActorPublisher[Any](impl) {
          override val wakeUpMsg = FanOut.SubstreamSubscribePending(id)
        }
        val publishers = Vector.tabulate(2)(factory)
        impl ! FanOut.ExposedPublishers(publishers)

        val inputs = Map[InPort, Subscriber[_]](
          tls.plainIn -> FanIn.SubInput[Any](impl, SslTlsCipherActor.UserIn),
          tls.cipherIn -> FanIn.SubInput[Any](impl, SslTlsCipherActor.TransportIn))

        val outputs = Map[OutPort, Publisher[_]](
          tls.plainOut -> publishers(SslTlsCipherActor.UserOut),
          tls.cipherOut -> publishers(SslTlsCipherActor.TransportOut))
        MaterializedModule(tls, (), inputs, outputs)

      case graph: GraphModule ⇒
        matGraph(graph, effectiveAttributes, matVal)

      case stage: GraphStageModule ⇒
        val graph =
          GraphModule(GraphAssembly(stage.shape.inlets, stage.shape.outlets, stage.stage),
            stage.shape, stage.attributes, Array(stage))
        matGraph(graph, effectiveAttributes, matVal)

      case junction: JunctionModule =>
        materializeJunction(junction, effectiveAttributes, effectiveSettings(effectiveAttributes))
    }

  }

  private def matGraph(graph: GraphModule, effectiveAttributes: Attributes, matVal: ju.Map[Module, Any]): Unit = {
    val calculatedSettings = effectiveSettings(effectiveAttributes)
    val (inHandlers, outHandlers, logics) = graph.assembly.materialize(effectiveAttributes, graph.matValIDs, matVal, registerSrc)

    val shell = new GraphInterpreterShell(graph.assembly, inHandlers, outHandlers, logics, graph.shape,
      calculatedSettings, this)

    val impl =
      if (subflowFuser != null && !effectiveAttributes.contains(Attributes.AsyncBoundary)) {
        subflowFuser(shell)
      } else {
        val props = ActorGraphInterpreter.props(shell)
        actorOf(props, stageName(effectiveAttributes), calculatedSettings.dispatcher)
      }

    for ((inlet, i) ← graph.shape.inlets.iterator.zipWithIndex) {
      val subscriber = new ActorGraphInterpreter.BoundarySubscriber(impl, shell, i)
      assignPort(inlet, subscriber)
    }
    for ((outlet, i) ← graph.shape.outlets.iterator.zipWithIndex) {
      val publisher = new ActorGraphInterpreter.BoundaryPublisher(impl, shell, i)
      impl ! ActorGraphInterpreter.ExposedPublisher(shell, i, publisher)
      assignPort(outlet, publisher)
    }
  }

  private def processorFor(op: StageModule,
                           effectiveAttributes: Attributes,
                           effectiveSettings: ActorMaterializerSettings): (Processor[Any, Any], Any) = op match {
    case DirectProcessor(processorFactory, _) => processorFactory()
    case Identity(attr)                       => (new VirtualProcessor, ())
    case _ =>
      val (opprops, mat) = ActorProcessorFactory.props(LocalMaterializerImpl.this, op, effectiveAttributes)
      ActorProcessorFactory[Any, Any](
            actorOf(opprops, stageName(effectiveAttributes), effectiveSettings.dispatcher)) -> mat
  }

  private def materializeJunction(
                                   op: JunctionModule,
                                   effectiveAttributes: Attributes,
                                   effectiveSettings: ActorMaterializerSettings): MaterializedModule = {
    op match {
      case fanin: FanIn =>
        val (props, inputs, output) = fanin match {

          case MergeModule(shape, _) =>
            (FairMerge.props(effectiveSettings, shape.inSeq.size), shape.inSeq, shape.out)

          case f: FlexiMergeModule[_, Shape] =>
            val flexi = f.flexi(f.shape)
            val shape: Shape = f.shape
            (FlexiMerge.props(effectiveSettings, f.shape, flexi), shape.inlets, shape.outlets.head)

          case MergePreferredModule(shape, _) =>
            (UnfairMerge.props(effectiveSettings, shape.inlets.size), shape.preferred +: shape.inSeq, shape.out)

          case ConcatModule(shape, _) =>
            require(shape.inSeq.size == 2, "currently only supporting concatenation of exactly two inputs") // TODO
            (Concat.props(effectiveSettings), shape.inSeq, shape.out)

          case zip: ZipWithModule =>
            (zip.props(effectiveSettings), zip.shape.inlets, zip.outPorts.head)
        }

        val impl = actorOf(props, stageName(effectiveAttributes), effectiveSettings.dispatcher)
        val publisher = new ActorPublisher[Any](impl)
        // Resolve cyclic dependency with actor. This MUST be the first message no matter what.
        impl ! ExposedPublisher(publisher)

        val inputMapping: Map[InPort, Subscriber[_]] = inputs.zipWithIndex.map { pair =>
          val (in, id) = pair
          (in, FanIn.SubInput[Any](impl, id))
        }.toMap

        val outMapping = Map(output -> publisher)
        MaterializedModule(fanin, (), inputMapping, outMapping)
      case fanout: FanOut =>
        val (props, in, outs) = fanout match {

          case r: GraphStage[Shape] =>
            val flexi = r.flexi(r.shape)
            val shape: Shape = r.shape
            (FlexiRoute.props(effectiveSettings, r.shape, flexi), shape.inlets.head: InPort, r.shape.outlets)

          case Broadcast(shape, eagerCancel) =>
            (Broadcast.props(effectiveSettings, eagerCancel, shape.outArray.size), shape.in, shape.outArray.toSeq)

          case BalanceModule(shape, waitForDownstreams, _) =>
            (Balance.props(effectiveSettings, shape.outArray.size, waitForDownstreams), shape.in, shape.outArray.toSeq)

          case unzip: UnzipWithModule =>
            (unzip.props(effectiveSettings), unzip.inPorts.head, unzip.shape.outlets)
        }
        val impl = actorOf(props, stageName(effectiveAttributes), effectiveSettings.dispatcher)
        val size = outs.size
        def factory(id: Int) =
          new ActorPublisher[Any](impl) {
            override val wakeUpMsg = FanOut.SubstreamSubscribePending(id)
          }
        val publishers =
          if (outs.size < 8) Vector.tabulate(size)(factory)
          else List.tabulate(size)(factory)

        impl ! FanOut.ExposedPublishers(publishers)
        val outputs: Map[OutPort, Publisher[_]] = publishers.iterator.zip(outs.iterator).map { case (pub, out) =>
          (out, pub)
        }.toMap

        val inputs: Map[InPort, Subscriber[_]] = Map(in -> ActorSubscriber[Any](impl))
        MaterializedModule(fanout, (), inputs, outputs)
    }
  }

  override def withNamePrefix(name: String): Materializer = {
    new LocalMaterializerImpl(system, settings, dispatchers, supervisor,
      haveShutDown, flowNameCounter, namePrefix = name, optimizations)
  }
}

object LocalMaterializerImpl {
  case class MaterializedModule(val module: Module, val matValue: Any, inputs: Map[InPort, Subscriber[_]] = Map.empty[InPort, Subscriber[_]] , outputs: Map[OutPort, Publisher[_]] = Map.empty[OutPort, Publisher[_]])

  def toFoldModule(reduce: ReduceModule[Any]): Fold[Any,Any] = {
    val f = reduce.f
    val aggregator = {(zero: Any, input: Any) =>
      if (zero == null) {
        input
      } else {
        f(zero, input)
      }
    }
    Fold(null, aggregator, null)
  }
}