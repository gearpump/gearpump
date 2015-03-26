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
package org.apache.gearpump.experiments.pipeline

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Delete, Get, HTable, Put, Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HColumnDescriptor, HTableDescriptor}

import scala.collection.JavaConversions._


class HBaseSink(taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {

  val hbaseConf = new Configuration
  val htd = new HTableDescriptor(TableName.valueOf(conf.getString(HBaseSink.TABLE_NAME).get))
  val hcd = new HColumnDescriptor(conf.getString(HBaseSink.TABLE_COLUMN_FAMILY).get)
  htd.addFamily(hcd)
  val tableName = htd.getName
  val table = new HTable(hbaseConf, tableName)
  table.setAutoFlush(false, false)

  override def onStart(newStartTime: StartTime): Unit = {
  }

  override def onNext(msg: Message): Unit = {
    insert(hcd.getNameAsString, hcd.getNameAsString, msg.msg.asInstanceOf[String])
  }

  override def onStop(): Unit = {
    table.close()
  }

    /**
   * Gets the entire contents of the HBase order table and list it to standard output.
   * 
   * @return the entire order table in ascending rowKey order.
   */
  def read:List[Result] = {
    val scan = new Scan
    val scanner = table.getScanner(scan)
    val scalaList: List[Result] = scanner.iterator.toList

    try {
      for (result <- scalaList) println("Scan 1234: " + result)
    } finally {
      scanner.close()
    }
    scalaList
  }

  /**
   * Returns a HBase result object containing the row and column family fields.
   * 
   * @return the row pointed to by rowKey
   */
  def findByKey(rowKey: String):Result = {
    val get = new Get(Bytes.toBytes(rowKey))
    val result = table.get(get)
    result
  }

  /**
   * Deletes all the row pointed to by rowKey including all its column families.
   */
  def deleteByKey(rowKey: String) {
    val delete = new Delete(Bytes.toBytes(rowKey))
    table.delete(delete)
  }

  /**
   * Inserts a new Order object into the HBase order table.  It creates the row with a
   * key of the current time stamp and returns that key to the caller.
   *
   * @param columnGroup – the column family that the columnName attribute belongs to
   * @param columnName – the columnName within the above column family
   * @param columnValue – the value for that column.
   *
   * @return – the key of the row just inserted
   */
  def insert(columnGroup: String, columnName: String, columnValue: String): String = {
    //Todo: redesign the rowkey
    val rowKey = System.currentTimeMillis.toString
    val row1 = Bytes.toBytes(rowKey)
    val p1 = new Put(row1)
    val databytes = Bytes.toBytes(columnGroup)
    p1.add(databytes, Bytes.toBytes(columnName), Bytes.toBytes(columnValue))
    table.put(p1)
    rowKey
  }

  /**
   * Updates the row pointed to by rowKey with the columnValue.
   * 
   * @param columnGroup – the column family that the columnName attribute belongs to
   * @param columnName – the columnName within the above column family
   * @param columnValue – the value for that column.
   */
  def update(rowKey: String, columnGroup: String, columnName: String, columnValue: String) = {
    val row = Bytes.toBytes(rowKey)
    val p = new Put(row)
    val databytes = Bytes.toBytes(columnGroup)
    p.add(databytes, Bytes.toBytes(columnName), Bytes.toBytes(columnValue))
    table.put(p)
    table.flushCommits()
  }

}

object HBaseSink {
  final val TABLE_NAME = "hbase.table.name"
  final val TABLE_COLUMN_FAMILY = "hbase.table.columnfamily"
}
