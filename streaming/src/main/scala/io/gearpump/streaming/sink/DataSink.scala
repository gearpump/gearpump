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

package io.gearpump.streaming.sink

import io.gearpump.Message
import io.gearpump.streaming.task.TaskContext

/**
 * Interface to implement custom data sink where result of a DAG is typically written
 * a DataSink could be a data store like HBase or simply a console
 *
 * An example would be like:
 * {{{
 *  class ConsoleSink extends DataSink[String] {
 *
 *    def open(context: TaskContext): Unit = {}
 *
 *    def write(s: String): Unit = {
 *      Console.println(s)
 *    }
 *
 *    def close(): Unit = {}
 *  }
 * }}}
 *
 * Subclass is required to be serializable
 */
trait DataSink extends java.io.Serializable {

  /**
   * Opens connection to data sink
   * invoked at onStart() method of [[io.gearpump.streaming.task.Task]]
   * @param context is the task context at runtime
   */
  def open(context: TaskContext): Unit

  /**
   * Writes message into data sink
   * invoked at onNext() method of [[io.gearpump.streaming.task.Task]]
   * @param message wraps data to be written out
   */
  def write(message: Message): Unit

  /**
   * Closes connection to data sink
   * invoked at onClose() method of [[io.gearpump.streaming.task.Task]]
   */
  def close(): Unit
}
