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

package io.gearpump.streaming.util

import akka.actor.{ActorPath, ActorRef}
import io.gearpump.streaming.task.TaskId

object ActorPathUtil {

  def executorActorName(executorId: Int): String = executorId.toString

  def taskActorName(taskId: TaskId): String = {
    s"processor_${taskId.processorId}_task_${taskId.index}"
  }

  def taskActorPath(appMaster: ActorRef, executorId: Int, taskId: TaskId): ActorPath = {
    val executorManager = appMaster.path.child(executorManagerActorName)
    val executor = executorManager.child(executorActorName(executorId))
    val task = executor.child(taskActorName(taskId))
    task
  }

  def executorManagerActorName: String = "executors"
}
