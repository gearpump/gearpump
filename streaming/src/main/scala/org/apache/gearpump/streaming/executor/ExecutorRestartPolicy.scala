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

package org.apache.gearpump.streaming.executor

import scala.collection.immutable
import scala.concurrent.duration.Duration

import org.apache.gearpump.streaming.task.TaskId
import org.apache.gearpump.util.RestartPolicy

/**
 *
 * Controls how many retries to recover failed executors.
 *
 * @param maxNrOfRetries the number of times a executor is allowed to be restarted,
 *                       negative value means no limit, if the limit is exceeded the policy
 *                       will not allow to restart the executor
 * @param withinTimeRange duration of the time window for maxNrOfRetries, Duration.Inf
 *                        means no window
 */
class ExecutorRestartPolicy(maxNrOfRetries: Int, withinTimeRange: Duration) {
  private var executorToTaskIds = Map.empty[Int, Set[TaskId]]
  private var taskRestartPolocies = new immutable.HashMap[TaskId, RestartPolicy]

  def addTaskToExecutor(executorId: Int, taskId: TaskId): Unit = {
    var taskSetForExecutorId = executorToTaskIds.getOrElse(executorId, Set.empty[TaskId])
    taskSetForExecutorId += taskId
    executorToTaskIds += executorId -> taskSetForExecutorId
    if (!taskRestartPolocies.contains(taskId)) {
      taskRestartPolocies += taskId -> new RestartPolicy(maxNrOfRetries, withinTimeRange)
    }
  }

  def allowRestartExecutor(executorId: Int): Boolean = {
    executorToTaskIds.get(executorId).map { taskIds =>
      taskIds.foreach { taskId =>
        taskRestartPolocies.get(taskId).map { policy =>
          if (!policy.allowRestart) {
            // scalastyle:off return
            return false
            // scalastyle:on return
          }
        }
      }
    }
    true
  }
}