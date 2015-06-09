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

import akka.actor.Actor
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.partitioner.PartitionerDescription
import org.apache.gearpump.streaming.appmaster.DAGManager.{GetTaskLaunchData, GetActiveDAG}
import org.apache.gearpump.streaming.appmaster.TaskSchedulerImpl.TaskLaunchData
import org.apache.gearpump.streaming.task.{TaskId, Subscriber}
import org.apache.gearpump.streaming.{StreamApplication, ProcessorDescription, DAG}
import org.apache.gearpump.util.Graph

/**
 * Will handle dag modification
 * @param userConfig
 */
class DAGManager(userConfig: UserConfig) extends Actor {
  private val dag: DAG =
    DAG(userConfig.getValue[Graph[ProcessorDescription, PartitionerDescription]](StreamApplication.DAG).get)

  def receive: Receive = {
    case GetActiveDAG =>
      dag
    case GetTaskLaunchData(taskId) =>
      val processorId = taskId.processorId
      val processorDescription = dag.processors(processorId)
      val subscriptions = Subscriber.of(processorId, dag)
      TaskLaunchData(taskId, processorDescription, subscriptions)
  }
}

object DAGManager {
  object GetActiveDAG
  case class GetTaskLaunchData(taskId: TaskId)
}
