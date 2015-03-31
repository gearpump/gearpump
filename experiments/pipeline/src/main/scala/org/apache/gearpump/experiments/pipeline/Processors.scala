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
import org.apache.gearpump.streaming.transaction.api.{MessageDecoder, TimeReplayableSource}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import upickle._

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.util.Try

// NOTE: Do not split up into separate files
// See http://stackoverflow.com/questions/28630780/upickle-and-scalajs-sealed-trait-serialisation
object Messages {

  case class Datum(dimension: String, metric: String, value: Double)

  case class Body(sample_id: String, source_id: String, event_ts: String, metrics: Array[Datum])

  case class Envelope(id: String, on: String, body: String)

}

object DatumHandler {
  val DEFAULT_INTERVAL = 2
  val CPU = "CPU"
  val CPU_INTERVAL = "pipeline.cpu.interval"
  val MEM = "MEM"
  val MEM_INTERVAL = "pipeline.memory.interval"

  implicit def handle(data: String): Datum = {
    read[Datum](data)
  }
  implicit def handle(datum: Datum): String = {
    write[Datum](datum)
  }
}

trait Average {
  this: Task =>
  var averageMem: Double = 0
  var totalCount: Long = 0
  var totalSum: Double = 0
  var timeStamp: TimeStamp = 0

  def timeInterval: Int

  def average(datum: Datum)(implicit timeStamp: TimeStamp): Option[Message] = {
    totalCount += 1
    totalSum += datum.value
    averageMem = totalSum/totalCount
    interval(datum)
  }

  def elapsedInSec(implicit ts: Long): Long = (ts - timeStamp)

  def interval(datum: Datum)(implicit ts: TimeStamp): Option[Message] = {
    timeStamp match {
      case 0 =>
        timeStamp = ts
        None
      case _ =>
        elapsedInSec match {
          case delta if delta > timeInterval =>
            timeStamp = ts
            Some(Message(write[Datum](Datum(datum.dimension,datum.metric,averageMem)), timeStamp))
          case _ =>
            None
        }
    }
  }
}

class CpuProcessor(taskContext: TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) with Average {
  import taskContext.output

  //TODO
  override def timeInterval = 100//conf.getInt(CPU_INTERVAL).getOrElse(DEFAULT_INTERVAL)

  override def onStart(newStartTime: StartTime): Unit = {
    LOG.info("starting")
  }

  override def onNext(msg: Message): Unit = {
    //Try({
      implicit val timeStamp = msg.timestamp
      val jsonData = msg.msg.asInstanceOf[String]
      val envelope = read[Envelope](jsonData)
      val body = read[Body](envelope.body)
      val metrics = body.metrics
      metrics.foreach(datum => {
        datum.dimension match {
          case CPU =>
            average(datum).foreach(output)
            //LOG.info(s"value=${datum.value}")
          case _ =>
        }
      })
    //}).failed.foreach(LOG.error("bad message", _))
  }
}

class MemoryProcessor(taskContext: TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) with Average {
  import taskContext.output

  override def timeInterval = 100//conf.getInt(MEM_INTERVAL).getOrElse(DEFAULT_INTERVAL)

  override def onStart(newStartTime: StartTime): Unit = {
    LOG.info("starting")
  }

  override def onNext(msg: Message): Unit = {
    //Try({
      implicit val timeStamp = msg.timestamp
      val jsonData = msg.msg.asInstanceOf[String]
      val envelope = read[Envelope](jsonData)
      val body = read[Body](envelope.body)
      val metrics = body.metrics
      metrics.foreach(datum => {
        datum.dimension match {
          case MEM =>
            average(datum).foreach(output)
            //LOG.info(s"value=${datum.value}")
          case _ =>
        }
      })
    //}).failed.foreach(LOG.error("bad message", _))
  }

}

class CpuPersistor(taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) with HBase {

  override def onStart(newStartTime: StartTime): Unit = {
    LOG.info("starting")
  }

  override def onNext(msg: Message): Unit = {
    //Try({
      val jsonData = msg.msg.asInstanceOf[String]
      val cpu: Datum = jsonData
      insert(hcd.getNameAsString, hcd.getNameAsString, cpu)
    //}).failed.foreach(LOG.error("bad message", _))
  }

}

class MemoryPersistor(taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) with HBase {

  override def onStart(newStartTime: StartTime): Unit = {
    LOG.info("starting")
  }

  override def onNext(msg: Message): Unit = {
    //Try({
      val jsonData = msg.msg.asInstanceOf[String]
      val memory: Datum = jsonData
      insert(hcd.getNameAsString, hcd.getNameAsString, memory)
    //}).failed.foreach(LOG.error("bad message", _))
  }

}

class KafkaProducer(taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {

  import taskContext.{output, parallelism, taskId}

  private val kafkaConfig = conf.getValue[KafkaConfig](KafkaConfig.NAME).get
  private val batchSize = kafkaConfig.getConsumerEmitBatchSize
  private val msgDecoder: MessageDecoder = kafkaConfig.getMessageDecoder

  val taskParallelism = parallelism

  private val source: TimeReplayableSource = new KafkaSource(taskContext.appName, taskId, taskParallelism,
    kafkaConfig, msgDecoder)
  private var startTime: TimeStamp = 0L

  override def onStart(newStartTime: StartTime): Unit = {
    Try({
      startTime = newStartTime.startTime
      LOG.info(s"start time is $startTime")
      source.setStartTime(startTime)
    }).failed.foreach(LOG.error("caught error", _))
    self ! Message("start", System.currentTimeMillis())
  }

  override def onNext(msg: Message): Unit = {
    Try({
      source.pull(batchSize).foreach(msg => {
        output(msg)
      })

    }).failed.foreach(LOG.error("caught error", _))
    self ! Message("continue", System.currentTimeMillis())
  }

  override def onStop(): Unit = {
    LOG.info("closing kafka source...")
    source.close()
  }
}

trait HBase {
  this: Task =>

  val ZOOKEEPER = "hbase.zookeeper.connect"
  val LOCAL_ZOOKEEPER = "127.0.0.1"
  val HBASE_ZOOKEEPER = "hbase.zookeeper.quorum"

  val hbaseConf = new Configuration
  private val kafkaConfig = taskConfig.getValue[KafkaConfig](KafkaConfig.NAME).get


  //TODO
  val zookeepers = taskConfig.getString(ZOOKEEPER).getOrElse(LOCAL_ZOOKEEPER)
  LOG.info(s"ZOOKEEPERS=$zookeepers")

  //hbaseConf.set(HBASE_ZOOKEEPER, taskConfig.getString(ZOOKEEPER).getOrElse(LOCAL_ZOOKEEPER))
  hbaseConf.set(HBASE_ZOOKEEPER, "ip-10-10-10-117.eu-west-1.compute.internal,ip-10-10-10-8.eu-west-1.compute.internal,ip-10-10-10-96.eu-west-1.compute.internal")

  val htd = new HTableDescriptor(TableName.valueOf("pipeline"))
  val hcd = new HColumnDescriptor("pipelineFamily")
  htd.addFamily(hcd)
  val table = new HTable(hbaseConf, htd.getName)

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




