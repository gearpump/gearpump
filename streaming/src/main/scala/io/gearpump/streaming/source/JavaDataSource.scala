/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import io.gearpump.streaming.javaapi.source.{DataSource => JavaSource}

class JavaDataSource(javaSource: JavaSource) extends DataSource {
  /**
   * open connection to data source
   * invoked in onStart() method of [[io.gearpump.streaming.source.DataSourceTask]]
   * @param context is the task context at runtime
   * @param startTime is the start time of system
   */
  override def open(context: TaskContext, startTime: Long): Unit = {
    javaSource.open(context, startTime)
  }

  /**
   * close connection to data source.
   * invoked in onStop() method of [[io.gearpump.streaming.source.DataSourceTask]]
   */
  override def close(): Unit = {
    javaSource.close()
  }

  /**
   * read a number of messages from data source.
   * invoked in each onNext() method of [[io.gearpump.streaming.source.DataSourceTask]]
   * @param batchSize max number of messages to read
   * @return an iterator of messages wrapped in [[io.gearpump.Message]]
   */
  override def read(batchSize: Int): Iterator[Message] = {
    new Iterator[Message] {
      val iterator: java.util.Iterator[Message] = javaSource.read(batchSize)

      override def hasNext: Boolean = {
        iterator.hasNext
      }

      override def next(): Message = {
        iterator.next()
      }
    }
  }
}
