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

package org.apache.gearpump.streaming.appmaster

import akka.actor.{ActorRef, Actor}
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.partitioner.{LifeTime, PartitionerDescription}
import org.apache.gearpump.streaming.appmaster.DAGManager.{DAGScheduled, WatchChange, DAGOperationSuccess, DAGOperationFailed, DAGReplace, TaskLaunchData, LatestDAG, GetTaskLaunchData, GetLatestDAG}
import org.apache.gearpump.streaming.task.{TaskId, Subscriber}
import org.apache.gearpump.streaming.{ProcessorId, StreamApplication, ProcessorDescription, DAG}
import org.apache.gearpump.util.Graph

/**
 * Will handle dag modification and other stuff related with DAG
 * @param userConfig
 */

class DAGManager(userConfig: UserConfig) extends Actor {
  implicit val system = context.system

  private var dags = List(DAG(userConfig.getValue[Graph[ProcessorDescription,
        PartitionerDescription]](StreamApplication.DAG).get))

  private var watcher: Option[ActorRef] = None

  private def taskLaunchData(dag: DAG, processorId: Int, context: AnyRef): TaskLaunchData = {
    val processorDescription = dag.processors(processorId)
    val subscribers = Subscriber.of(processorId, dag)
    TaskLaunchData(processorDescription, subscribers, context)
  }

  def receive: Receive = {
    case GetLatestDAG =>
      sender ! LatestDAG(dags.last)
    case GetTaskLaunchData(version, processorId, context) =>
      dags.find(_.version == version).foreach { dag =>
        sender ! taskLaunchData(dag, processorId, context)
      }
    case DAGReplace(oldProcessorId, newProcessor) =>
      if (dags.length > 1) {
        sender !  DAGOperationFailed("We are in the process of handling previous dynamic dag change")
      } else {
        val oldDAG = dags.last
        val newVersion = oldDAG.version + 1
        val newDAG = replaceDAG(oldDAG, oldProcessorId, newProcessor, newVersion)
        dags :+= newDAG
        watcher.foreach(_ ! LatestDAG(newDAG))
        sender ! DAGOperationSuccess
      }

    case WatchChange(watcher) =>
      this.watcher = Some(watcher)

    case DAGScheduled(version) =>
      // filter out obsolete versions.
      dags = dags.filter(_.version > version)
  }

  private def replaceDAG(dag: DAG, oldProcessorId: ProcessorId, newProcessor: ProcessorDescription, newVersion: Int): DAG = {
    val newProcessorLife = newProcessor.life
    val oldProcessorLife = dag.processors(oldProcessorId).life.copy(die = newProcessorLife.birth)
    var newProcessorMap = dag.processors + (newProcessor.id -> newProcessor)
    newProcessorMap += oldProcessorId -> dag.processors(oldProcessorId).copy(life = oldProcessorLife)

    val newGraph = Graph.empty[ProcessorId, PartitionerDescription]
    newGraph.addVertex(newProcessor.id)
    dag.graph.incomingEdgesOf(oldProcessorId).foreach { nodeEdgeNode =>
      val (upstreamProcessorId, partitioner, _) = nodeEdgeNode
      newGraph.addVertex(upstreamProcessorId)
      newGraph.addEdge(upstreamProcessorId, partitioner, newProcessor.id)
    }

    dag.graph.outgoingEdgesOf(oldProcessorId).foreach { nodeEdgeNode =>
      val (_, partitioner, downstreamProcessorId) = nodeEdgeNode
      newGraph.addVertex(downstreamProcessorId)
      newGraph.addEdge(newProcessor.id, partitioner, downstreamProcessorId)
    }

    new DAG(newVersion, newProcessorMap, dag.graph.addGraph(newGraph))
  }
}

object DAGManager {
  case class WatchChange(watcher: ActorRef)

  object GetLatestDAG
  case class LatestDAG(dag: DAG)

  case class GetTaskLaunchData(version: Int, processorId: Int, context: AnyRef = null)
  case class TaskLaunchData(processorDescription : ProcessorDescription, subscribers: List[Subscriber], context: AnyRef = null)

  case class DAGReplace(oldProcessorId: ProcessorId, newProcessorDescription: ProcessorDescription)

  sealed trait DAGOperationResult
  case object DAGOperationSuccess
  case class DAGOperationFailed(reason: String)

  case class DAGScheduled(dagVersion: Int)
}