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

import akka.actor.ActorRef
import io.gearpump.cluster.appmaster.WorkerInfo
import io.gearpump.cluster.scheduler.Resource
import io.gearpump.streaming.task.TaskId
import io.gearpump.transport.HostPort

object ExecutorToAppMaster {
  case class RegisterExecutor(
      executor: ActorRef, executorId: Int, resource: Resource, worker : WorkerInfo)

  case class RegisterTask(taskId: TaskId, executorId: Int, task: HostPort)
  case class UnRegisterTask(taskId: TaskId, executorId: Int)

  case class MessageLoss(executorId: Int, taskId: TaskId,
      cause: String, ex: Option[Throwable] = None)
}
