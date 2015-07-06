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

import org.apache.gearpump._
import org.apache.gearpump.streaming.source.DataSource
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{HTable, ResultScanner, Scan}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ArrayBuffer

/**
 * Read data from HBase.
 * The result Message is each cell(rowkey, family, column, value) as an array byte[][4].
 * The message timestamp is the timestamp of the cell if useCurrentTime is false. Otherwise, it is current system time.
 */
class HBaseDataSource(hbaseConf : Configuration, tableName : String, families: Array[String], useCurrentTime : Boolean) extends DataSource {
  var table : Option[HTable] = None
  var scanner: Option[ResultScanner] = None
  var timeStamp : Option[TimeStamp] = None
  /**
   * open connection to data source
   * invoked in onStart() method of [[org.apache.gearpump.streaming.task.Task]]
   * @param context is the task context at runtime
   * @param startTime is the start time of system
   */
  def open(context: TaskContext, startTime: Option[TimeStamp]): Unit = {
    //if not use current time, then the startTime means the start time stamp in HBase.
    if(!useCurrentTime)
      timeStamp = startTime
    table = Some(new HTable(hbaseConf, tableName))
    restartScanner
  }

  /**
   * read a number of messages from data source.
   * invoked in each onNext() method of [[org.apache.gearpump.streaming.task.Task]]
   * @param batchSize the max number of messages to return
   * @return a list of messages wrapped in [[Message]]
   */
  def read(batchSize : Int): List[Message] = {
    var count = 0
    val buffer = ArrayBuffer[Message]()
    var hasNext = true
    while(count < batchSize && hasNext) {
      val row =  scanner.map(_.next())
      if(row==None) {
        hasNext = false
      } else {
        val rowkey: Array[Byte] = row.get.getRow()
        val cellScanner = row.get.cellScanner()
        while (cellScanner.advance()) {
          count = count + 1
          val cell = cellScanner.current()
          val cellResult = new Array[Array[Byte]](4)
          cellResult(0) = rowkey
          cellResult(1) = CellUtil.cloneFamily(cell)
          cellResult(2) = CellUtil.cloneQualifier(cell)
          cellResult(3) = CellUtil.cloneValue(cell)
          val msg = Message(cellResult, (if(useCurrentTime) System.currentTimeMillis() else cell.getTimestamp()))
          buffer += msg

          //record the max timestamp for this scan
          //will restart scan when this scan finished from this timestamp
          //assume timestamp is monotonic
          if (timeStamp.getOrElse(0L) < cell.getTimestamp())
            timeStamp = Some(cell.getTimestamp())
        }
      }
    }

    if(!hasNext)
      restartScanner

    buffer.toList
  }

  private def closeScanner = {
    scanner.map(_.close)
    scanner = None
  }

  private def restartScanner = {
    closeScanner
    val scan = new Scan()
    timeStamp.foreach(scan.setTimeRange(_, Long.MaxValue))
    families.foreach { family: String =>
      scan.addFamily(Bytes.toBytes(family))
    }
    scanner = table.map(_.getScanner(scan))
  }
  /**
   * close connection to data source
   * invoked in onStop() method of [[org.apache.gearpump.streaming.task.Task]]
   */
  def close(): Unit = {
    closeScanner
    table.map(_.close)
    table = None
  }
}
