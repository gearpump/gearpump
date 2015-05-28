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

import org.apache.gearpump.streaming.appmaster.DAGManager.GetTaskLaunchData
import org.apache.gearpump.streaming.appmaster.TaskManager.ChangeTasksOnExecutor
import org.apache.gearpump.streaming.{ExecutorId, ProcessorId}
import org.apache.gearpump.streaming.appmaster.TaskRegistry.{TaskLocation, Reject, Accept, RegisterTaskStatus, ProcessorParallism}
import org.apache.gearpump.streaming.task.{TaskLocations, TaskId}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

class TaskRegistry(appId: Int, expectedTasks: List[TaskId],
    var registeredTasks: Map[TaskId, TaskLocation] = Map.empty[TaskId, TaskLocation]) {

  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)

  private val totalTaskCount = expectedTasks.size

  private val processors = expectedTasks.map(_.processorId).toSet

  def registerTask(taskId: TaskId, location: TaskLocation): RegisterTaskStatus = {
    val processorId = taskId.processorId

    if (processors.contains(processorId)) {
      LOG.info(s"Task $taskId has been Launched for app $appId")
      registeredTasks += taskId -> location
      Accept
    } else {
      LOG.error(s" the task is not accepted for registration, taskId: ${taskId}")
      Reject
    }
  }

  def getTaskLocations: TaskLocations = {
    val taskLocations =  registeredTasks.toList.groupBy(_._2.host).mapValues{ v =>
      val taskIds = v.map(_._1)
      taskIds.toSet
    }
    TaskLocations(taskLocations)
  }

  def getExecutorId(taskId: TaskId): Option[Int] = {
    registeredTasks.get(taskId).map(_.executorId)
  }

  def isAllTasksRegistered: Boolean = {
    totalTaskCount == registeredTasks.size
  }

  private def filterTasks(processorId: ProcessorId): List[TaskId] = {
    registeredTasks.keys.toList.filter(_.processorId == processorId)
  }

  def processorExecutors(processorId: ProcessorId): Map[ExecutorId, List[TaskId]] = {
    val taskToExecutor = filterTasks(processorId).flatMap { taskId =>
      getExecutorId(taskId).map { executorId =>
        (taskId, executorId)
      }
    }

    val executorToTasks = taskToExecutor.groupBy(_._2).mapValues(_.map(_._1))
    executorToTasks
  }
}

object TaskRegistry {
  case class ProcessorParallism(processorId: ProcessorId, parallism: Int)

  sealed trait RegisterTaskStatus
  object Accept extends RegisterTaskStatus
  object Reject extends RegisterTaskStatus

  case class TaskLocation(executorId: Int, host: HostPort)
}