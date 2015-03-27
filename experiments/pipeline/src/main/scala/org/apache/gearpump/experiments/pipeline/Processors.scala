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

import org.apache.gearpump._
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.experiments.pipeline.DatumHandler._
import org.apache.gearpump.experiments.pipeline.Messages._
import org.apache.gearpump.streaming.kafka.KafkaSource
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}
import org.apache.gearpump.streaming.transaction.api.{TimeReplayableSource, TimeStampFilter, MessageDecoder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.metrics
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import upickle._

import scala.collection.JavaConversions._
import scala.reflect.ClassTag


// NOTE: Do not split up into separate files
// See http://stackoverflow.com/questions/28630780/upickle-and-scalajs-sealed-trait-serialisation
object Messages {

  case class Datum(dimension: String, metric: String, value: Double)

  case class Body(source_id: String, event_ts: String, metrics: Array[Datum])

  case class Envelope(id: Long, on: String, body: String)

}


object DatumHandler {
  implicit def handle(data: String): Datum = {
    read[Datum](data)
  }
}


class CpuProcessor(taskContext: TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {

  import taskContext.output

  override def onStart(newStartTime: StartTime): Unit = {
  }

  override def onNext(msg: Message): Unit = {
    val jsonData = msg.msg.asInstanceOf[String]
    val body = read[Body](jsonData)
    val metrics = body.metrics
    metrics.foreach(datum => {
      datum.dimension match {
        case "CPU" =>
          output(new Message(write(datum), msg.timestamp))
        case _ =>
      }
    })

  }

  override def onStop(): Unit = {
  }
}

class CpuPersistor(taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) with HBase {

  override def onStart(newStartTime: StartTime): Unit = {
  }

  override def onNext(msg: Message): Unit = {
    val jsonData = msg.msg.asInstanceOf[String]
    val cpu: Datum = jsonData

    insert(hcd.getNameAsString, hcd.getNameAsString, write(cpu))
  }

}

class MemoryProcessor(taskContext: TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {

  import taskContext.output

  override def onStart(newStartTime: StartTime): Unit = {
  }

  override def onNext(msg: Message): Unit = {
    val jsonData = msg.msg.asInstanceOf[String]
    val body = read[Body](jsonData)
    val metrics = body.metrics
    metrics.foreach(datum => {
      datum.dimension match {
        case "MEM" =>
          output(new Message(write(datum), msg.timestamp))
        case _ =>
      }
    })

  }

  override def onStop(): Unit = {
  }
}

class MemoryPersistor(taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) with HBase {

  override def onStart(newStartTime: StartTime): Unit = {
  }

  override def onNext(msg: Message): Unit = {
    val jsonData = msg.msg.asInstanceOf[String]
    val mem: Datum = jsonData

    insert(hcd.getNameAsString, hcd.getNameAsString, write(mem))
  }

}

class KafkaProducer(taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {

  import taskContext.{output, parallelism, taskId}

  private val kafkaConfig = conf.getValue[KafkaConfig](KafkaConfig.NAME).get
  private val batchSize = kafkaConfig.getConsumerEmitBatchSize
  private val msgDecoder: MessageDecoder = kafkaConfig.getMessageDecoder
  private val filter: TimeStampFilter = kafkaConfig.getTimeStampFilter

  val taskParallelism = parallelism

  private val source: TimeReplayableSource = new KafkaSource(taskContext.appName, taskId, taskParallelism,
    kafkaConfig, msgDecoder)
  private var startTime: TimeStamp = 0L

  def testParser(): Unit = {
    try {
      var jsonData = "{\"source_id\":\"src-4\",\"event_ts\":\"2015-03-26T18:26:57.112853223Z\",\"metrics\":[{\"dimension\":\"CPU\",\"metric\":\"total\",\"value\":29005},{\"dimension\":\"CPU\",\"metric\":\"user\",\"value\":271},{\"dimension\":\"CPU\",\"metric\":\"sys\",\"value\":424},{\"dimension\":\"LOAD\",\"metric\":\"min\",\"value\":0.04},{\"dimension\":\"MEM\",\"metric\":\"free\",\"value\":15625940992},{\"dimension\":\"MEM\",\"metric\":\"used\",\"value\":144596992}]}"
      read[Body](jsonData)
      LOG.info("test parsed body")
    } catch {
      case t:Throwable =>
        LOG.error("caught error", t)
    }
  }
  override def onStart(newStartTime: StartTime): Unit = {
    startTime = newStartTime.startTime
    LOG.info(s"start time $startTime")
    source.setStartTime(startTime)
    testParser()
    self ! Message("start", System.currentTimeMillis())
  }

  override def onNext(msg: Message): Unit = {
    try {
      source.pull(batchSize).foreach(msg => {
        LOG.info("got msg")
        val jsonData = msg.msg.asInstanceOf[String]
        val body = read[Body](jsonData)
        LOG.info("parsed body")
        output(msg)
      })
    } catch {
      case t: Throwable =>
        LOG.error("caught error", t)
    }

    self ! Message("continue", System.currentTimeMillis())
  }

  override def onStop(): Unit = {
    LOG.info("closing kafka source...")
    source.close()
  }
}


trait HBase {
  val hbaseConf = new Configuration
  val htd = new HTableDescriptor(TableName.valueOf("pipeline"))
  val hcd = new HColumnDescriptor("pipelineFamily")
  htd.addFamily(hcd)
  val tableName = htd.getName
  val table = new HTable(hbaseConf, tableName)

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
    table.close()
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
    val rowKey = System.currentTimeMillis.toString
    val row1 = Bytes.toBytes(rowKey)
    val p1 = new Put(row1)
    val databytes = Bytes.toBytes(columnGroup)
    p1.add(databytes, Bytes.toBytes(columnName), Bytes.toBytes(columnValue))
    table.put(p1)
    table.flushCommits()
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




