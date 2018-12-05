/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.streaming

import io.gearpump.Time.MilliSeconds
import io.gearpump.streaming.appmaster.TaskRegistry.TaskLocations
import io.gearpump.streaming.task.{Subscriber, TaskId}

object AppMasterToExecutor {
  case class LaunchTasks(
      taskId: List[TaskId], dagVersion: Int, processorDescription: ProcessorDescription,
      subscribers: List[Subscriber])

  case object TasksLaunched

  /**
   * dagVersion, life, and subscribers will be changed on target task list.
   */
  case class ChangeTasks(
      taskId: List[TaskId], dagVersion: Int, life: LifeTime, subscribers: List[Subscriber])

  case class TasksChanged(taskIds: List[TaskId])

  case class ChangeTask(
      taskId: TaskId, dagVersion: Int, life: LifeTime, subscribers: List[Subscriber])

  case class TaskChanged(taskId: TaskId, dagVersion: Int)

  case class StartTask(taskId: TaskId)

  case class StopTask(taskId: TaskId)

  case class TaskLocationsReady(taskLocations: TaskLocations, dagVersion: Int)

  case class TaskLocationsReceived(dagVersion: Int, executorId: ExecutorId)

  case class TaskLocationsRejected(
      dagVersion: Int, executorId: ExecutorId, reason: String, ex: Throwable)

  case class StartAllTasks(dagVersion: Int)

  case class StartDynamicDag(dagVersion: Int)
  case class TaskRegistered(taskId: TaskId, sessionId: Int, startClock: MilliSeconds)
  case class TaskRejected(taskId: TaskId)

  case object RestartClockService
  class MsgLostException extends Exception
}
