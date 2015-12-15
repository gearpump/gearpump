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

package io.gearpump.streaming.appmaster

import akka.actor.{ActorRef, Actor}
import io.gearpump.streaming._
import io.gearpump.streaming.task.Subscriber
import io.gearpump.cluster.UserConfig
import io.gearpump.partitioner.{PartitionerDescription}
import DagManager.{NewDAGDeployed, WatchChange, DAGOperationSuccess, DAGOperationFailed, ReplaceProcessor, TaskLaunchData, LatestDAG, GetTaskLaunchData, GetLatestDAG}
import io.gearpump.util.{LogUtil, Graph}
import org.slf4j.Logger

/**
 * Will handle dag modification and other stuff related with DAG
 * @param userConfig
 */

class DagManager(appId: Int, userConfig: UserConfig, dag: Option[DAG]) extends Actor {
  private val NOT_INITIALIZED = -1

  def this(appId: Int, userConfig: UserConfig) = {
    this(appId, userConfig, None)
  }

  implicit val system = context.system

  var dags = List(
    dag.getOrElse(DAG(userConfig.getValue[Graph[ProcessorDescription,
      PartitionerDescription]](StreamApplication.DAG).get))
  )

  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)

  private var maxProcessorId = {
    val keys = dags.head.processors.keys
    if (keys.size == 0) {
      0
    }else {
      keys.max
    }
  }

  private def nextProcessorId: ProcessorId = {
    maxProcessorId += 1
    maxProcessorId
  }

  private var watchers = List.empty[ActorRef]

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
        LOG.info(s"Get task launcher data for processor: $processorId, dagVersion: $version")
        sender ! taskLaunchData(dag, processorId, context)
      }
    case ReplaceProcessor(oldProcessorId, inputNewProcessor) =>
      var newProcessor = inputNewProcessor.copy(id = nextProcessorId)
      if (inputNewProcessor.jar == null){
        val oldJar = dags.last.processors.get(oldProcessorId).get
        newProcessor = newProcessor.copy(jar = oldJar.jar)
      }
      if (dags.length > 1) {
        sender !  DAGOperationFailed("We are in the process of handling previous dynamic dag change")
      } else {
        val oldDAG = dags.last
        val newVersion = oldDAG.version + 1
        val newDAG = replaceDAG(oldDAG, oldProcessorId, newProcessor, newVersion)
        dags :+= newDAG

        LOG.info(s"ReplaceProcessor old: $oldProcessorId, new: $newProcessor")
        LOG.info(s"new DAG: $newDAG")
        watchers.foreach(_ ! LatestDAG(newDAG))
        sender ! DAGOperationSuccess
      }

    case WatchChange(watcher) =>
      if (!this.watchers.contains(watcher)) {
        this.watchers :+= watcher
      }

    case NewDAGDeployed(dagVersion) =>
      // means the new DAG version has been successfully deployed.
      // remove obsolete versions.
      if (dagVersion != NOT_INITIALIZED) {
        dags = dags.filter(_.version == dagVersion)
      }
  }

  private def replaceDAG(dag: DAG, oldProcessorId: ProcessorId, newProcessor: ProcessorDescription, newVersion: Int): DAG = {

    val oldProcessorLife = LifeTime(dag.processors(oldProcessorId).life.birth, newProcessor.life.birth)

    val newProcessorMap = dag.processors ++
      Map(oldProcessorId -> dag.processors(oldProcessorId).copy(life = oldProcessorLife),
        newProcessor.id -> newProcessor)

    val newGraph = dag.graph.subGraph(oldProcessorId).
      replaceVertex(oldProcessorId, newProcessor.id).addGraph(dag.graph)
    new DAG(newVersion, newProcessorMap, newGraph)
  }
}

object DagManager {
  case class WatchChange(watcher: ActorRef)

  case object GetLatestDAG
  case class LatestDAG(dag: DAG)


  case class GetTaskLaunchData(dagVersion: Int, processorId: Int, context: AnyRef = null)
  case class TaskLaunchData(processorDescription : ProcessorDescription, subscribers: List[Subscriber], context: AnyRef = null)

  sealed trait DAGOperation

  case class ReplaceProcessor(oldProcessorId: ProcessorId, newProcessorDescription: ProcessorDescription) extends DAGOperation

  sealed trait DAGOperationResult
  case object DAGOperationSuccess extends DAGOperationResult
  case class DAGOperationFailed(reason: String) extends DAGOperationResult

  case class NewDAGDeployed(dagVersion: Int)
}