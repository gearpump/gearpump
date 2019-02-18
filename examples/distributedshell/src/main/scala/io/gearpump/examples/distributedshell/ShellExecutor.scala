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

package io.gearpump.examples.distributedshell

import akka.actor.Actor
import io.gearpump.cluster.ExecutorContext
import io.gearpump.examples.distributedshell.DistShellAppMaster.{ShellCommand, ShellCommandResult}
import io.gearpump.util.LogUtil
import org.slf4j.Logger
import scala.sys.process._
import scala.util.{Failure, Success, Try}

/** Executor actor on remote machine */
class ShellExecutor(executorContext: ExecutorContext) extends Actor {
  import executorContext._
  private val LOG: Logger = LogUtil.getLogger(getClass, executor = executorId, app = appId)

  LOG.info(s"ShellExecutor started!")

  override def receive: Receive = {
    case ShellCommand(command) =>
      val process = Try(s"$command".!!)
      val result = process match {
        case Success(msg) => msg
        case Failure(ex) => ex.getMessage
      }
      sender ! ShellCommandResult(executorId, result)
  }
}
