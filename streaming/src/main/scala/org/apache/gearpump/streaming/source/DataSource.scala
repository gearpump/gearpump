/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming.source

import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.{TimeStamp, Message}

/**
 * interface to implement custom source where data is read into the system.
 * a DataSource could be a message queue like kafka or simply data generation source.
 *
 * an example would be like
 * {{{
 *  GenStringSource extends DataSource {
 *
 *    def open(context: TaskContext, startTime: Option[TimeStamp]): Unit = {}
 *
 *    def read(): List[Message] = {
 *      List(Message("message-1"), Message("message-2"), Message("message-3")
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
   * open connection to data source
   * invoked in onStart() method of [[org.apache.gearpump.streaming.task.Task]]
   * @param context is the task context at runtime
   * @param startTime is the start time of system
   */
  def open(context: TaskContext, startTime: TimeStamp): Unit

  /**
   * read a number of messages from data source.
   * invoked in each onNext() method of [[org.apache.gearpump.streaming.task.Task]]
   * @return a list of messages wrapped in [[Message]]
   */
  def read(): List[Message]

  /**
   * close connection to data source
   * invoked in onStop() method of [[org.apache.gearpump.streaming.task.Task]]
   */
  def close(): Unit
}
