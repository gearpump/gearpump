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

package io.gearpump.streaming.source

import io.gearpump.Message
import io.gearpump.streaming.task.TaskContext
import java.time.Instant

/**
 * Interface to implement custom source where data is read into the system.
 * a DataSource could be a message queue like kafka or simply data generation source.
 *
 * An example would be like
 * {{{
 *  GenMsgSource extends DataSource {
 *
 *    def open(context: TaskContext, startTime: TimeStamp): Unit = {}
 *
 *    def read(context: TaskContext): Message = {
 *      Message("message")
 *    }
 *
 *    def close(): Unit = {}
 *  }
 * }}}
 *
 * subclass is required to be serializable
 */
trait DataSource extends java.io.Serializable {


  /**
   * Opens connection to data source
   * invoked in onStart() method of [[io.gearpump.streaming.source.DataSourceTask]]
   *
   * @param context is the task context at runtime
   * @param startTime is the start time of system
   */
  def open(context: TaskContext, startTime: Instant): Unit

  /**
   * Reads next message from data source and
   * returns null if no message is available
   *
   * @return a [[io.gearpump.Message]] or null
   */
  def read(): Message

  /**
   * Closes connection to data source.
   * invoked in onStop() method of [[io.gearpump.streaming.source.DataSourceTask]]
   */
  def close(): Unit

  /**
   * Returns a watermark such that no timestamp earlier than the watermark should enter the system
   * Watermark.MAX mark the end of source data
   */
  def getWatermark: Instant
}
