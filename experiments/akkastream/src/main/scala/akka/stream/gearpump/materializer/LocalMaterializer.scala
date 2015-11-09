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

import akka.actor.{ActorCell, ActorRef, ActorSystem, Deploy, LocalActorRef, PoisonPill, Props, RepointableActorRef}
import akka.dispatch.Dispatchers
import akka.pattern.ask
import akka.stream.ModuleGraph.Edge
import akka.stream.impl.StreamLayout.Module
import akka.stream.impl.{FlowNameCounter, StreamSupervisor}
import akka.stream.{ActorAttributes, ActorMaterializer, ActorMaterializerSettings, Attributes, ClosedShape, Graph => AkkaGraph, MaterializationContext, ModuleGraph, Optimizations}
import io.gearpump.util.Graph

import scala.concurrent.{Await, ExecutionContextExecutor}

/**
 * [[LocalMaterializer]] will use local actor to materialize the graph
 * Use LocalMaterializer.apply to construct the LocalMaterializer.
 *
 * It is functional equivalent to [[akka.stream.impl.ActorMaterializerImpl]]
 *
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
abstract class LocalMaterializer(
    val system: ActorSystem,
    override val settings: ActorMaterializerSettings,
    dispatchers: Dispatchers,
    val supervisor: ActorRef,
    val haveShutDown: AtomicBoolean,
    flowNameCounter: AtomicLong,
    namePrefix: String,
    optimizations: Optimizations) extends ActorMaterializer {

  override def effectiveSettings(opAttr: Attributes): ActorMaterializerSettings = {
    import ActorAttributes._
    import Attributes._
    opAttr.attributeList.foldLeft(settings) { (s, attr) =>
      attr match {
        case InputBuffer(initial, max)    => s.withInputBuffer(initial, max)
        case Dispatcher(dispatcher)       => s.withDispatcher(dispatcher)
        case SupervisionStrategy(decider) => s.withSupervisionStrategy(decider)
        case l: LogLevels                 => s
        case Name(_)                      => s
        case other => s
      }
    }
  }

  override def shutdown(): Unit =
    if (haveShutDown.compareAndSet(false, true)) supervisor ! PoisonPill

  override def isShutdown: Boolean = haveShutDown.get()

  private[akka] def actorOf(props: Props, name: String, dispatcher: String): ActorRef = {
    supervisor match {
      case ref: LocalActorRef =>
        ref.underlying.attachChild(props.withDispatcher(dispatcher), name, systemService = false)
      case ref: RepointableActorRef =>
        if (ref.isStarted)
          ref.underlying.asInstanceOf[ActorCell].attachChild(props.withDispatcher(dispatcher), name, systemService = false)
        else {
          implicit val timeout = ref.system.settings.CreationTimeout
          val f = (supervisor ? StreamSupervisor.Materialize(props.withDispatcher(dispatcher), name)).mapTo[ActorRef]
          Await.result(f, timeout.duration)
        }
      case unknown =>
        throw new IllegalStateException(s"Stream supervisor must be a local actor, was [${unknown.getClass.getName}]")
    }
  }

  override lazy val executionContext: ExecutionContextExecutor = dispatchers.lookup(settings.dispatcher match {
    case Deploy.NoDispatcherGiven => Dispatchers.DefaultDispatcherId
    case other                    => other
  })

  def materialize(graph: Graph[Module, Edge], inputMatValues: Map[Module, Any]): Map[Module, Any]

  override def materialize[Mat](runnableGraph: AkkaGraph[ClosedShape, Mat]): Mat = {
    val graph = ModuleGraph(runnableGraph)
    val matValues = materialize(graph.graph, Map.empty[Module, Any])
    graph.resolve(matValues)
  }

  override def actorOf(context: MaterializationContext, props: Props): ActorRef = {
    val dispatcher =
      if (props.deploy.dispatcher == Deploy.NoDispatcherGiven) effectiveSettings(context.effectiveAttributes).dispatcher
      else props.dispatcher
    actorOf(props, context.stageName, dispatcher)
  }
}

object LocalMaterializer {

  def apply(materializerSettings: Option[ActorMaterializerSettings] = None, namePrefix: Option[String] = None, optimizations: Optimizations = Optimizations.none)(implicit system: ActorSystem): LocalMaterializerImpl  = {

    val settings = materializerSettings getOrElse ActorMaterializerSettings(system)
    apply(settings, namePrefix.getOrElse("flow"), optimizations)(system)
  }

  def apply(materializerSettings: ActorMaterializerSettings, namePrefix: String, optimizations: Optimizations)(implicit system: ActorSystem): LocalMaterializerImpl = {
    val haveShutDown = new AtomicBoolean(false)

    new LocalMaterializerImpl(
      system,
      materializerSettings,
      system.dispatchers,
      system.actorOf(StreamSupervisor.props(materializerSettings, haveShutDown).withDispatcher(materializerSettings.dispatcher)),
      haveShutDown,
      FlowNameCounter(system).counter,
      namePrefix,
      optimizations)
  }
}
