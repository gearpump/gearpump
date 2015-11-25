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
import akka.stream.gearpump.materializer.LocalMaterializer
import akka.stream.gearpump.module.{SinkBridgeModule, SourceBridgeModule}
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.impl.StreamLayout.Module
import akka.stream.impl.{PublisherSource, SubscriberSink}
import akka.stream.{Outlet, SinkShape, SourceShape}
import io.gearpump.util.Graph
import org.reactivestreams.{Publisher, Subscriber}

/**
 *
 * [[LocalGraph]] is a [[SubGraph]] of the application DSL Graph, which only
 *  contain module that can be materialized in local JVM.
 *
 * @param graph
 */
class LocalGraph(override val graph: Graph[Module, Edge]) extends SubGraph

object LocalGraph {

  /**
   * materialize LocalGraph in local JVM
   * @param system
   */
  class LocalGraphMaterializer(system: ActorSystem) extends SubGraphMaterializer {

    // create a local materializer
    val materializer = LocalMaterializer()(system)

    /**
     *
     * @param matValues Materialized Values for each module before materialization
     * @return Materialized Values for each Module after the materialization.
     */
    override def materialize(graph: SubGraph, matValues: Map[Module, Any]): Map[Module, Any] = {
      val newGraph: Graph[Module, Edge] = graph.graph.mapVertex{ module =>
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
      materializer.materialize(newGraph, matValues)
    }

    override def shutdown: Unit = {
      materializer.shutdown()
    }
  }
}

