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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.Dispatchers
import akka.stream.ModuleGraph.{Edge, MaterializedValueSourceAttribute}
import akka.stream.actor.ActorSubscriber
import akka.stream.gearpump.materializer.LocalMaterializerImpl.MaterializedModule
import akka.stream.gearpump.util.MaterializedValueOps
import akka.stream.impl.GenJunctions.{UnzipWithModule, ZipWithModule}
import akka.stream.impl.Junctions.{BalanceModule, BroadcastModule, ConcatModule, FanInModule, FanOutModule, FlexiMergeModule, FlexiRouteModule, JunctionModule, MergeModule, MergePreferredModule}
import akka.stream.impl.Stages.{DirectProcessor, Identity, StageModule}
import akka.stream.impl.StreamLayout.Module
import akka.stream.impl.io.SslTlsCipherActor
import akka.stream.impl.{ActorProcessorFactory, ActorPublisher, Balance, Broadcast, Concat, ExposedPublisher, FairMerge, FanIn, FanOut, FlexiMerge, FlexiRoute, MaterializedValuePublisher, MaterializedValueSource, SinkModule, SourceModule, UnfairMerge, VirtualProcessor}
import akka.stream.io.SslTls.TlsModule
import akka.stream.{ActorMaterializerSettings, Attributes, Graph => AkkaGraph, InPort, MaterializationContext, Materializer, Optimizations, OutPort, Shape}
import io.gearpump.util.Graph
import org.reactivestreams.{Processor, Publisher, Subscriber}

/**
 * This materializer is functional equivalent to [[akka.stream.impl.ActorMaterializerImpl]]
 *
 * @param system
 * @param settings
 * @param dispatchers
 * @param supervisor
 * @param haveShutDown
 * @param flowNameCounter
 * @param namePrefix
 * @param optimizations
 */
class LocalMaterializerImpl (
    system: ActorSystem,
    settings: ActorMaterializerSettings,
    dispatchers: Dispatchers,
    supervisor: ActorRef,
    haveShutDown: AtomicBoolean,
    flowNameCounter: AtomicLong,
    namePrefix: String,
    optimizations: Optimizations)
  extends LocalMaterializer(
    system, settings, dispatchers, supervisor,
    haveShutDown, flowNameCounter, namePrefix, optimizations){

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
  }

  private def publishToMaterializedValueSource(modules: List[MaterializedModule], matValues: Map[Module, Any]) = {
    modules.foreach { module =>
      val source = module.module.asInstanceOf[MaterializedValueSource[_]]
      val attr = source.attributes.getAttribute(classOf[MaterializedValueSourceAttribute], null)

      Option(attr).map { attr =>
        val valueToPublish = MaterializedValueOps(attr.mat).resolve[Any](matValues)
        module.outputs.foreach { portAndPublisher =>
          val (port, publisher) = portAndPublisher
          publisher match {
            case valuePublisher: MaterializedValuePublisher =>
              valuePublisher.setValue(valueToPublish)
          }
        }
      }
    }
  }

  private[this] def nextFlowNameCount(): Long = flowNameCounter.incrementAndGet()

  private[this] def createFlowName(): String = s"$namePrefix-${nextFlowNameCount()}"

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
      case matValue: MaterializedValueSource[_] =>
        val pub = new MaterializedValuePublisher
        val outputs = Map[OutPort, Publisher[_]](matValue.shape.outlet -> pub)
        MaterializedModule(matValue, (), outputs = outputs)
      case sink: SinkModule[_, _] =>
        val (sub, mat) = sink.create(newMaterializationContext())
        val inputs = Map[InPort, Subscriber[_]](sink.shape.inlet -> sub)
        MaterializedModule(sink, mat, inputs)
      case source: SourceModule[_, _] =>
        val (pub, mat) = source.create(newMaterializationContext())
        val outputs = Map[OutPort, Publisher[_]](source.shape.outlet -> pub)
        MaterializedModule(source, mat, outputs = outputs)
      case stage: StageModule =>
        val (processor, mat) = processorFor(stage, effectiveAttributes, effectiveSettings(effectiveAttributes))
        val inputs = Map[InPort, Subscriber[_]](stage.inPort -> processor)
        val outputs = Map[OutPort, Publisher[_]](stage.outPort -> processor)
        MaterializedModule(stage, mat, inputs, outputs)
      case tls: TlsModule => // TODO solve this so TlsModule doesn't need special treatment here
        val es = effectiveSettings(effectiveAttributes)
        val props =
          SslTlsCipherActor.props(es, tls.sslContext, tls.firstSession, tracing = false, tls.role, tls.closing)
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

      case junction: JunctionModule =>
        materializeJunction(junction, effectiveAttributes, effectiveSettings(effectiveAttributes))
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
    case fanin: FanInModule =>
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

      val inputMapping: Map[InPort, Subscriber[_]] = inputs.zipWithIndex.map{ pair =>
        val (in, id) = pair
        (in, FanIn.SubInput[Any](impl, id))
      }.toMap

      val outMapping = Map(output -> publisher)
      MaterializedModule(fanin, (), inputMapping, outMapping)

    case fanout: FanOutModule =>
      val (props, in, outs) = fanout match {

        case r: FlexiRouteModule[t, Shape] =>
          val flexi = r.flexi(r.shape)
          val shape: Shape = r.shape
          (FlexiRoute.props(effectiveSettings, r.shape, flexi), shape.inlets.head: InPort, r.shape.outlets)

        case BroadcastModule(shape, eagerCancel, _) =>
          (Broadcast.props(effectiveSettings, eagerCancel, shape.outArray.size), shape.in, shape.outArray.toSeq)

        case BalanceModule(shape, waitForDownstreams, _) =>
          (Balance.props(effectiveSettings, shape.outArray.size, waitForDownstreams), shape.in, shape.outArray.toSeq)

        case unzip: UnzipWithModule =>
          (unzip.props(effectiveSettings), unzip.inPorts.head, unzip.shape.outlets)
      }
      val impl = actorOf(props, stageName(effectiveAttributes), effectiveSettings.dispatcher)
      val size = outs.size
      def factory(id: Int) =
        new ActorPublisher[Any](impl) { override val wakeUpMsg = FanOut.SubstreamSubscribePending(id) }
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
}