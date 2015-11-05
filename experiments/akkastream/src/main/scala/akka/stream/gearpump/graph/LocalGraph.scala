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

package akka.stream.gearpump.graph

import akka.actor.ActorSystem
import akka.stream.ModuleGraph.Edge
import akka.stream.gearpump.impl.LocalMaterializerImpl
import akka.stream.gearpump.module.BridgeModule.{SinkBridgeModule, SourceBridgeModule}
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.impl.StreamLayout.{CompositeModule, MaterializedValueNode, Module}
import akka.stream.impl.{PublisherSource, SubscriberSink}
import akka.stream.scaladsl.RunnableGraph
import akka.stream.{ActorMaterializer, Attributes, ClosedShape, InPort, OutPort, Outlet, SinkShape, SourceShape}
import io.gearpump.util.Graph
import org.reactivestreams.{Publisher, Subscriber}

class LocalGraph(graph: Graph[Module, Edge]) extends SubGraph {

  override def materialize(matValues: Map[Module, Any], system: ActorSystem): Map[Module, Any] = {

    val newGraph: Graph[Module, Edge] = graph.mapVertex{ module =>
      module match {
        case source: SourceBridgeModule[AnyRef, AnyRef] =>
          val subscriber = matValues(source).asInstanceOf[Subscriber[AnyRef]]
          val shape = SinkShape(source.inPort)
          new SubscriberSink(subscriber, DefaultAttributes.subscriberSink, shape)
        case sink: SinkBridgeModule[AnyRef, AnyRef] =>
          val publisher = matValues(sink).asInstanceOf[Publisher[AnyRef]]
          val shape = SourceShape(sink.outPort.asInstanceOf[Outlet[AnyRef]])
          new PublisherSource(publisher, DefaultAttributes.publisherSource, shape)
        case other =>
          other
      }
    }

    val materializer = LocalMaterializerImpl()(system)
    val newMatValues = materializer.materialize(newGraph)
    matValues ++ newMatValues
  }
}