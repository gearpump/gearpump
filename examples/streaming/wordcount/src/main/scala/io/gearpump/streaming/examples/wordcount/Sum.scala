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

package io.gearpump.streaming.examples.wordcount

import akka.actor.Cancellable
import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.task.{Task, TaskContext}
import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

class Sum(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  private[wordcount] val map: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()

  private[wordcount] var wordCount: Long = 0
  private var snapShotTime: Long = System.currentTimeMillis()
  private var snapShotWordCount: Long = 0

  private var scheduler: Cancellable = null

  override def onStart(startTime: Instant): Unit = {
    scheduler = taskContext.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
      new FiniteDuration(30, TimeUnit.SECONDS))(reportWordCount)
  }

  override def onNext(msg: Message): Unit = {
    if (null != msg) {
      val current = map.getOrElse(msg.value.asInstanceOf[String], 0L)
      wordCount += 1
      map.put(msg.value.asInstanceOf[String], current + 1)
    }
  }

  override def onStop(): Unit = {
    if (scheduler != null) {
      scheduler.cancel()
    }
  }

  def reportWordCount(): Unit = {
    val current: Long = System.currentTimeMillis()
    LOG.info(s"Task ${taskContext.taskId} Throughput:" +
      s" ${(wordCount - snapShotWordCount, (current - snapShotTime) / 1000)} (words, second)")
    snapShotWordCount = wordCount
    snapShotTime = current
  }
}