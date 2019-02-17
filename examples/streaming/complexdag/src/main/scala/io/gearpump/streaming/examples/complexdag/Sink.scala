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
import io.gearpump.streaming.task.{Task, TaskContext}
import java.time.Instant
import scala.collection.mutable

class Sink(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf) {

  var list = mutable.MutableList[String]()

  override def onStart(startTime: Instant): Unit = {
    list += getClass.getCanonicalName
  }

  override def onNext(msg: Message): Unit = {
    val l = msg.value.asInstanceOf[Vector[String]]
    list.size match {
      case 1 =>
        l.foreach(f => {
          list += f
        })
      case _ =>
    }
  }
}
