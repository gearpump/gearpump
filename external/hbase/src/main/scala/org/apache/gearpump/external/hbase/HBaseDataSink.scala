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

package org.apache.gearpump.external.hbase

import org.apache.gearpump.Message
import org.apache.gearpump.streaming.sink.DataSink
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HTable, Put}

/**
 * Use HBase as Data Sink target.
 * Assume the target table already exists.
 * It accept Message with [[org.apache.hadoop.hbase.client.Put]] as payload.
 */
class HBaseDataSink(hbaseConf : Configuration, tableName : String) extends DataSink{
  var table : Option[HTable] = None
  /**
   * open connection to data sink
   * invoked at onStart() method of [[org.apache.gearpump.streaming.task.Task]]
   * @param context is the task context at runtime
   */
  def open(context: TaskContext): Unit = {
    table = Some(new HTable(hbaseConf, tableName))
  }

  /**
   * write message into data sink
   * invoked at onNext() method of [[org.apache.gearpump.streaming.task.Task]]
   * @param message wraps data to be written out
   */
  def write(message: Message): Unit = {
    message match {
      case Message(put: Put, timestamp) => {
        table.map(_.put(put))
      }
    }
  }

  /**
   * close connection to data sink
   * invoked at onClose() method of [[org.apache.gearpump.streaming.task.Task]]
   */
  override def close(): Unit = {
    table.map(_.close)
    table = None
  }
}
