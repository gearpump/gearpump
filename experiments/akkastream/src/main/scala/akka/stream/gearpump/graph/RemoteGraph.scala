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
import akka.stream.gearpump.materializer.RemoteMaterializerImpl
import akka.stream.gearpump.module.{SinkBridgeModule, SourceBridgeModule}
import akka.stream.gearpump.task.SinkBridgeTask.SinkBridgeTaskClient
import akka.stream.gearpump.task.SourceBridgeTask.SourceBridgeTaskClient
import akka.stream.impl.StreamLayout.Module
import io.gearpump.cluster.client.ClientContext
import io.gearpump.streaming.ProcessorId
import io.gearpump.util.Graph

/**
 *
 * [[RemoteGraph]] is a [[SubGraph]] of the application DSL Graph, which only
 *  contain modules that can be materialized in remote Gearpump cluster.
 *
 * By calling [[materialize]], [[RemoteGraph]] will materialize all sub-modules
 * remotely as a stream application in Gearpump Cluster.
 *
 * @param graph
 */
class RemoteGraph(override val graph: Graph[Module, Edge]) extends SubGraph {

  /**
   * @see [[SubGraph.materialize]]
   *
   */
  override def materialize(inputMatValues: Map[Module, Any], system: ActorSystem): Map[Module, Any] = {
    if (graph.isEmpty) {
      inputMatValues
    } else {
      doMaterialize(inputMatValues, system)
    }
  }

  private def doMaterialize(inputMatValues: Map[Module, Any], system: ActorSystem): Map[Module, Any] = {
    val materializer = new RemoteMaterializerImpl(graph, system)
    val (app, matValues) = materializer.materialize
    val context = ClientContext(system)
    val appId = context.submit(app)
    println("sleep 5 second until the applicaiton is ready on cluster")
    Thread.sleep(5000)

    def resolve(matValues: Map[Module, ProcessorId]): Map[Module, Any] = {
      matValues.toList.flatMap{ kv =>
        val (module, processorId) = kv
        module match {
          case source: SourceBridgeModule[AnyRef, AnyRef] =>
            val bridge = new SourceBridgeTaskClient[AnyRef](system, context, appId, processorId)
            Some((module, bridge))
          case sink: SinkBridgeModule[AnyRef, AnyRef] =>
            val bridge = new SinkBridgeTaskClient(system, context, appId, processorId)
            Some((module, bridge))
          case other =>
            None
        }
      }.toMap
    }

    inputMatValues ++ resolve(matValues)
  }
}
