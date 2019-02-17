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

package io.gearpump.streaming.task

import io.gearpump.Message
import io.gearpump.streaming.dsl.window.impl.{StreamingOperator, TimestampedValue}
import java.time.Instant

object TaskUtil {

  /**
   * Resolves a classname to a Task class.
   *
   * @param className  the class name to resolve
   * @return resolved class
   */
  def loadClass(className: String): Class[_ <: Task] = {
    val loader = Thread.currentThread().getContextClassLoader()
    loader.loadClass(className).asSubclass(classOf[Task])
  }

  def trigger[IN, OUT](watermark: Instant, runner: StreamingOperator[IN, OUT],
      context: TaskContext): Unit = {
    val triggeredOutputs = runner.trigger(watermark)
    context.updateWatermark(triggeredOutputs.watermark)
    triggeredOutputs.outputs.foreach { case TimestampedValue(v, t) =>
      context.output(Message(v, t))
    }
  }

  /**
   * @return t1 if t1 is not larger than t2 and t2 otherwise
   */
  def min(t1: Instant, t2: Instant): Instant = {
    if (t1.isAfter(t2)) t2
    else t1
  }

  /**
   * @return t1 if t1 is not smaller than t2 and t2 otherwise
   */
  def max(t1: Instant, t2: Instant): Instant = {
    if (t2.isBefore(t1)) t1
    else t2
  }
}
