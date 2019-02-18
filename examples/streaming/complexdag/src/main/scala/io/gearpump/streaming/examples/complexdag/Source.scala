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

package io.gearpump.streaming.examples.complexdag

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.source.Watermark
import io.gearpump.streaming.task.{Task, TaskContext}
import java.time.Instant

class Source(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  import taskContext.output

  override def onStart(startTime: Instant): Unit = {
    self ! Watermark(Instant.now)
  }

  override def onNext(msg: Message): Unit = {
    val list = Vector(getClass.getCanonicalName)
    val now = Instant.now
    output(Message(list, now.toEpochMilli))
    self ! Watermark(now)
  }
}

