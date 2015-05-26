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
import org.apache.gearpump.streaming.appmaster.DagManager.{DAGScheduled, WatchChange, DAGOperationSuccess, DAGOperationFailed, ReplaceProcessor, TaskLaunchData, LatestDAG, GetTaskLaunchData, GetLatestDAG}
import org.apache.gearpump.streaming.task.{TaskId, Subscriber}
import org.apache.gearpump.streaming.{ProcessorId, StreamApplication, ProcessorDescription, DAG}
import org.apache.gearpump.util.{LogUtil, Graph}
import org.slf4j.Logger

/**
 * Will handle dag modification and other stuff related with DAG
 * @param userConfig
 */

class DagManager(appId: Int, userConfig: UserConfig) extends Actor {
  implicit val system = context.system

  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)

  private var dags = List(DAG(userConfig.getValue[Graph[ProcessorDescription,
        PartitionerDescription]](StreamApplication.DAG).get))

  private var maxProcessorId = dags.head.processors.keys.max

  private def nextProcessorId: ProcessorId = {
    maxProcessorId += 1
    maxProcessorId
  }

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
    case ReplaceProcessor(oldProcessorId, inputNewProcessor) =>
      val newProcessor = inputNewProcessor.copy(id = nextProcessorId)
      if (dags.length > 1) {
        sender !  DAGOperationFailed("We are in the process of handling previous dynamic dag change")
      } else {
        val oldDAG = dags.last
        val newVersion = oldDAG.version + 1
        val newDAG = replaceDAG(oldDAG, oldProcessorId, newProcessor, newVersion)
        dags :+= newDAG

        LOG.info(s"ReplaceProcessor old: $oldProcessorId, new: $newProcessor")
        LOG.info(s"new DAG: $newDAG")
        watcher.foreach(_ ! LatestDAG(newDAG))
        sender ! DAGOperationSuccess
      }

    case WatchChange(watcher) =>
      this.watcher = Some(watcher)

    case DAGScheduled(version) =>
      // filter out obsolete versions.
      dags = dags.filter(_.version >= version)
  }

  private def replaceDAG(dag: DAG, oldProcessorId: ProcessorId, newProcessor: ProcessorDescription, newVersion: Int): DAG = {

    val oldProcessorLife = LifeTime(dag.processors(oldProcessorId).life.birth, newProcessor.life.birth)

    val newProcessorMap = dag.processors ++
      Map(oldProcessorId -> dag.processors(oldProcessorId).copy(life = oldProcessorLife),
        newProcessor.id -> newProcessor)

    val newGraph = dag.graph.subGraph(oldProcessorId).replaceVertex(oldProcessorId, newProcessor.id)
    new DAG(newVersion, newProcessorMap, newGraph.addGraph(dag.graph))
  }
}

object DagManager {
  case class WatchChange(watcher: ActorRef)

  object GetLatestDAG
  case class LatestDAG(dag: DAG)

  case class GetTaskLaunchData(version: Int, processorId: Int, context: AnyRef = null)
  case class TaskLaunchData(processorDescription : ProcessorDescription, subscribers: List[Subscriber], context: AnyRef = null)

  sealed trait DAGOperation

  case class ReplaceProcessor(oldProcessorId: ProcessorId, newProcessorDescription: ProcessorDescription) extends DAGOperation

  sealed trait DAGOperationResult
  case object DAGOperationSuccess extends DAGOperationResult
  case class DAGOperationFailed(reason: String) extends DAGOperationResult

  case class DAGScheduled(dagVersion: Int)
}