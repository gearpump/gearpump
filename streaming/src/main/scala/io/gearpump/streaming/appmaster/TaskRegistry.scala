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

import org.slf4j.Logger

import io.gearpump.cluster.scheduler.Resource
import io.gearpump.streaming.appmaster.ExecutorManager.ExecutorResourceUsageSummary
import io.gearpump.streaming.appmaster.TaskRegistry._
import io.gearpump.streaming.task.TaskId
import io.gearpump.streaming.{ExecutorId, ProcessorId}
import io.gearpump.transport.HostPort
import io.gearpump.util.LogUtil

/**
 * TaskRegistry is used to track the registration of all tasks
 * when one application is booting up.
 */
class TaskRegistry(val expectedTasks: List[TaskId],
    var registeredTasks: Map[TaskId, TaskLocation] = Map.empty[TaskId, TaskLocation],
    var deadTasks: Set[TaskId] = Set.empty[TaskId]) {

  private val LOG: Logger = LogUtil.getLogger(getClass)

  private val processors = expectedTasks.map(_.processorId).toSet

  /**
   * When a task is booted, it need to call registerTask to register itself.
   * If this task is valid, then accept it, otherwise reject it.
   *
   */
  def registerTask(taskId: TaskId, location: TaskLocation): RegisterTaskStatus = {
    val processorId = taskId.processorId

    if (processors.contains(processorId)) {
      registeredTasks += taskId -> location
      Accept
    } else {
      LOG.error(s" the task is not accepted for registration, taskId: ${taskId}")
      Reject
    }
  }

  def copy(expectedTasks: List[TaskId] = this.expectedTasks,
      registeredTasks: Map[TaskId, TaskLocation] = this.registeredTasks,
      deadTasks: Set[TaskId] = this.deadTasks): TaskRegistry = {
    new TaskRegistry(expectedTasks, registeredTasks, deadTasks)
  }

  def getTaskLocations: TaskLocations = {
    val taskLocations = registeredTasks.toList.groupBy(_._2.host).map { pair =>
      val (k, v) = pair
      val taskIds = v.map(_._1)
      (k, taskIds.toSet)
    }
    TaskLocations(taskLocations)
  }

  def getTaskExecutorMap: Map[TaskId, ExecutorId] = {
    getTaskLocations.locations.flatMap { pair =>
      val (hostPort, taskSet) = pair
      taskSet.map { taskId =>
        (taskId, getExecutorId(taskId).getOrElse(-1))
      }
    }
  }

  def getExecutorId(taskId: TaskId): Option[Int] = {
    registeredTasks.get(taskId).map(_.executorId)
  }

  def executors: List[ExecutorId] = {
    registeredTasks.toList.map(_._2.executorId)
  }

  def isAllTasksRegistered: Boolean = {
    val aliveTasks = (expectedTasks.toSet -- deadTasks)
    aliveTasks.forall(task => registeredTasks.contains(task))
  }

  def isTaskRegisteredForExecutor(executorId: ExecutorId): Boolean = {
    registeredTasks.exists(_._2.executorId == executorId)
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

    val executorToTasks = taskToExecutor.groupBy(_._2).map { kv =>
      val (k, v) = kv
      (k, v.map(_._1))
    }
    executorToTasks
  }

  def usedResource: ExecutorResourceUsageSummary = {
    val resourceMap = registeredTasks.foldLeft(Map.empty[ExecutorId, Resource]) { (map, task) =>
      val resource = map.getOrElse(task._2.executorId, Resource(0)) + Resource(1)
      map + (task._2.executorId -> resource)
    }
    ExecutorResourceUsageSummary(resourceMap)
  }
}

object TaskRegistry {
  sealed trait RegisterTaskStatus
  case object Accept extends RegisterTaskStatus
  case object Reject extends RegisterTaskStatus

  case class TaskLocation(executorId: Int, host: HostPort)

  case class TaskLocations(locations: Map[HostPort, Set[TaskId]])
}