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
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes

class HBaseSink(tableName: String) extends DataSink{
  lazy val hbaseConf = HBaseConfiguration.create()
  lazy val table = new HTable(hbaseConf, tableName)

  override def open(context: TaskContext): Unit = {}

  def insert(put: Put): Unit = {
    table.put(put)
  }

  def insert(rowKey: String, columnGroup: String, columnName: String, value: String): Unit = {
    insert(Bytes.toBytes(rowKey), Bytes.toBytes(columnGroup), Bytes.toBytes(columnName), Bytes.toBytes(value))
  }

  def insert(rowKey: Array[Byte], columnGroup: Array[Byte], columnName: Array[Byte], value: Array[Byte]): Unit = {
    val put = new Put(rowKey)
    put.add(columnGroup, columnName, value)
    insert(put)
  }

  def put(msg: AnyRef): Unit = {
    msg match {
      case seq: Seq[AnyRef] =>
        seq.foreach(put)
      case put: Put =>
        insert(put)
      case tuple: (String, String, String, String) =>
        insert(tuple._1, tuple._2, tuple._3, tuple._4)
      case tuple: (Array[Byte], Array[Byte], Array[Byte], Array[Byte]) =>
        insert(tuple._1, tuple._2, tuple._3, tuple._4)
    }
  }

  override def write(message: Message): Unit = {
    put(message.msg)
  }

  def close(): Unit = {
    table.close()
  }
}

object HBaseSink {
  val HBASESINK = "hbasesink"
  val TABLE_NAME = "hbase.table.name"
  val COLUMN_FAMILY = "hbase.table.column.family"
  val COLUMN_NAME = "hbase.table.column.name"
  def apply(tableName: String): HBaseSink = new HBaseSink(tableName)
}
