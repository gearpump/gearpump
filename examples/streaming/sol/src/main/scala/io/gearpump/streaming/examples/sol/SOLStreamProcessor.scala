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

package io.gearpump.streaming.examples.sol

import akka.actor.Cancellable
import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.task.{Task, TaskContext}
import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

class SOLStreamProcessor(taskContext: TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {
  import taskContext.output

  val taskConf = taskContext

  private var msgCount: Long = 0
  private var scheduler: Cancellable = null
  private var snapShotWordCount: Long = 0
  private var snapShotTime: Long = 0

  override def onStart(startTime: Instant): Unit = {
    scheduler = taskContext.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
      new FiniteDuration(5, TimeUnit.SECONDS))(reportWordCount())
    snapShotTime = System.currentTimeMillis()
  }

  override def onNext(msg: Message): Unit = {
    output(msg)
    msgCount = msgCount + 1
  }

  override def onStop(): Unit = {
    if (scheduler != null) {
      scheduler.cancel()
    }
  }

  def reportWordCount(): Unit = {
    val current: Long = System.currentTimeMillis()
    LOG.info(s"Task ${taskConf.taskId} " +
      s"Throughput: ${(msgCount - snapShotWordCount, (current - snapShotTime) / 1000)} " +
      s"(words, second)")
    snapShotWordCount = msgCount
    snapShotTime = current
  }
}
