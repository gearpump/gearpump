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
import org.apache.gearpump.akkastream.GearpumpMaterializer.Edge
import org.apache.gearpump.akkastream.materializer.RemoteMaterializerImpl
import org.apache.gearpump.akkastream.module.{SinkBridgeModule, SourceBridgeModule}
import org.apache.gearpump.akkastream.task.SinkBridgeTask.SinkBridgeTaskClient
import org.apache.gearpump.akkastream.task.SourceBridgeTask.SourceBridgeTaskClient
import akka.stream.impl.StreamLayout.Module
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.embedded.EmbeddedCluster
import org.apache.gearpump.streaming.ProcessorId
import org.apache.gearpump.util.Graph

/**
 *
 * [[RemoteGraph]] is a [[SubGraph]] of the application DSL Graph, which only
 *  contain modules that can be materialized in remote Gearpump cluster.
 *
 * @param graph Graph
 */
class RemoteGraph(override val graph: Graph[Module, Edge]) extends SubGraph

object RemoteGraph {

  /**
   * * materialize LocalGraph in remote gearpump cluster
   * @param useInProcessCluster Boolean
   * @param system ActorSystem
   */
  class RemoteGraphMaterializer(useInProcessCluster: Boolean, system: ActorSystem)
    extends SubGraphMaterializer {
    private val local = if (useInProcessCluster) {
      Some(EmbeddedCluster())
    } else {
      None
    }

    private val context: ClientContext = local match {
      case Some(l) => l.newClientContext
      case None => ClientContext(null)
    }

    override def materialize(subGraph: SubGraph,
        inputMatValues: scala.collection.mutable.Map[Module, Any]):
        scala.collection.mutable.Map[Module, Any] = {
      val graph = subGraph.graph
      
      if (graph.isEmpty) {
        inputMatValues
      } else {
        doMaterialize(graph: Graph[Module, Edge], inputMatValues)
      }
    }

    private def doMaterialize(graph: Graph[Module, Edge],
        inputMatValues: scala.collection.mutable.Map[Module, Any]):
        scala.collection.mutable.Map[Module, Any] = {
      val materializer = new RemoteMaterializerImpl(graph, system)
      val (app, matValues) = materializer.materialize

      val appId = context.submit(app).appId
      // scalastyle:off println
      println("sleep 5 second until the application is ready on cluster")
      // scalastyle:on println
      Thread.sleep(5000)

      def resolve(matValues: Map[Module, ProcessorId]): Map[Module, Any] = {
        matValues.toList.flatMap { kv =>
          val (module, processorId) = kv
          module match {
            case source: SourceBridgeModule[_, _] =>
              val bridge = new SourceBridgeTaskClient[AnyRef](system.dispatcher,
                context, appId, processorId)
              Some((module, bridge))
            case sink: SinkBridgeModule[_, _] =>
              val bridge = new SinkBridgeTaskClient(system, context, appId, processorId)
              Some((module, bridge))
            case other =>
              None
          }
        }.toMap
      }

      inputMatValues ++ resolve(matValues)
    }

    override def shutdown: Unit = {
      context.close()
      local.foreach(_.stop())
    }
  }
}
