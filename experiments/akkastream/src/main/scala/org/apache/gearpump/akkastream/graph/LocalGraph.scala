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

package org.apache.gearpump.akkastream.graph

import akka.actor.ActorSystem
import akka.stream.impl.Stages.DefaultAttributes
import akka.stream.impl.StreamLayout.Module
import akka.stream.impl.{PublisherSource, SubscriberSink}
import akka.stream.{SinkShape, SourceShape}
import org.apache.gearpump.akkastream.GearpumpMaterializer.Edge
import org.apache.gearpump.akkastream.materializer.LocalMaterializerImpl
import org.apache.gearpump.akkastream.module.{SinkBridgeModule, SourceBridgeModule}
import org.apache.gearpump.util.Graph
import org.reactivestreams.{Publisher, Subscriber}

/**
 *
 * [[LocalGraph]] is a [[SubGraph]] of the application DSL Graph, which only
 *  contain module that can be materialized in local JVM.
 *
 * @param graph Graph[Module, Edge]
 */
class LocalGraph(override val graph: Graph[Module, Edge]) extends SubGraph

object LocalGraph {

  /**
   * materialize LocalGraph in local JVM
   * @param system ActorSystem
   */
  class LocalGraphMaterializer(system: ActorSystem) extends SubGraphMaterializer {

    // create a local materializer
    val materializer = LocalMaterializerImpl()(system)

    /**
     *
     * @param matValues Materialized Values for each module before materialization
     * @return Materialized Values for each Module after the materialization.
     */
    override def materialize(graph: SubGraph,
        matValues: scala.collection.mutable.Map[Module, Any]):
        scala.collection.mutable.Map[Module, Any] = {
      val newGraph: Graph[Module, Edge] = graph.graph.mapVertex {
        case source: SourceBridgeModule[in, out] =>
          val subscriber = matValues(source).asInstanceOf[Subscriber[in]]
          val shape: SinkShape[in] = SinkShape(source.inPort)
          new SubscriberSink(subscriber, DefaultAttributes.subscriberSink, shape)
        case sink: SinkBridgeModule[in, out] =>
          val publisher = matValues(sink).asInstanceOf[Publisher[out]]
          val shape: SourceShape[out] = SourceShape(sink.outPort)
          new PublisherSource(publisher, DefaultAttributes.publisherSource, shape)
        case other =>
          other
      }
      materializer.materialize(newGraph, matValues)
    }

    override def shutdown: Unit = {
      materializer.shutdown()
    }
  }
}

