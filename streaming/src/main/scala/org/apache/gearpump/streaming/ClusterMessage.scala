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

package org.apache.gearpump.streaming

import akka.actor.{Actor, ActorRef}
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.appmaster.WorkerInfo
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.streaming.task.{Subscriber, TaskActor, Task, TaskContextData, TaskId}
import org.apache.gearpump.transport.HostPort
import scala.language.existentials

object AppMasterToExecutor {
  case class LaunchTasks(taskId: List[TaskId], dagVersion: Int, processorDescription: ProcessorDescription, subscribers: List[Subscriber])

  case object TasksLaunched

  /**
   * dagVersion, life, and subscribers will be changed on target task list.
   */
  case class ChangeTasks(taskId: List[TaskId], dagVersion: Int, life: LifeTime, subscribers: List[Subscriber])

  case object TasksChanged

  case class ChangeTask(taskId: TaskId, dagVersion: Int, life: LifeTime, subscribers: List[Subscriber])

  case class TaskChanged(taskId: TaskId, dagVersion: Int)

  case class StartClock(clock : TimeStamp)
  case object TaskRejected

  case object RestartClockService
  class RestartException extends Exception
  class MsgLostException extends Exception
}

object ExecutorToAppMaster {
  case class RegisterExecutor(executor: ActorRef, executorId: Int, resource: Resource, worker : WorkerInfo)

  case class RegisterTask(taskId: TaskId, executorId : Int, task: HostPort)
}

object AppMasterToMaster {
  case class StallingTasks(tasks: List[TaskId])
}