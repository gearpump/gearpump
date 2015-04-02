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

import com.typesafe.config.Config
import org.apache.gearpump._
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.experiments.hbase.HBaseSinkInterface
import org.apache.gearpump.experiments.pipeline.Messages._
import org.apache.gearpump.streaming.kafka.KafkaSource
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}
import org.apache.gearpump.streaming.transaction.api.{MessageDecoder, TimeReplayableSource}
import org.apache.hadoop.conf.Configuration
import upickle._

import scala.language.implicitConversions
import scala.util.Try

// NOTE: Do not split up into separate files
// See http://stackoverflow.com/questions/28630780/upickle-and-scalajs-sealed-trait-serialisation
object Messages {
  val PIPELINE = "pipeline"
  val HBASESINK = "hbasesink"
  val DEFAULT_INTERVAL = 2
  val CPU = "CPU"
  val CPU_INTERVAL = "pipeline.cpu.interval"
  val MEM = "MEM"
  val MEM_INTERVAL = "pipeline.memory.interval"

  case class Datum(dimension: String, metric: String, value: Double) extends java.io.Serializable

  case class Body(sample_id: String, source_id: String, event_ts: String, metrics: Array[Datum])

  case class Envelope(id: String, on: String, body: String)
}

case class PipelineConfig(config: Config) extends Serializable

object PipelineConfig {
  implicit def getConfig(pipelineConfig: PipelineConfig): Config = pipelineConfig.config
}


trait Average {
  this: Task =>
  var averageMem: Double = 0
  var totalCount: Long = 0
  var totalSum: Double = 0
  var timeStamp: TimeStamp = 0

  def timeInterval: Int

  def average(datum: Datum)(implicit timeStamp: TimeStamp): Option[Datum] = {
    totalCount += 1
    totalSum += datum.value
    averageMem = totalSum/totalCount
    interval(datum)
  }

  def elapsedInSec(implicit ts: Long): Long = ts - timeStamp

  def interval(datum: Datum)(implicit ts: TimeStamp): Option[Datum] = {
    timeStamp match {
      case 0 =>
        timeStamp = ts
        None
      case _ =>
        elapsedInSec match {
          case delta if delta > timeInterval =>
            timeStamp = ts
            Some(Datum(datum.dimension,datum.metric,averageMem))
          case _ =>
            None
        }
    }
  }
}

class CpuProcessor(taskContext: TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) with Average {

  import taskContext.output

  val pipelineConfig = conf.getValue[PipelineConfig](PIPELINE)

  override def timeInterval = pipelineConfig.map(config => {
    config.getInt(CPU_INTERVAL)
  }).getOrElse(DEFAULT_INTERVAL)

  override def onStart(newStartTime: StartTime): Unit = {
    LOG.info("starting timeInterval=$timeInterval")
  }

  override def onNext(msg: Message): Unit = {
    Try({
      implicit val timeStamp = msg.timestamp
      val jsonData = msg.msg.asInstanceOf[String]
      val metrics = read[Array[Datum]](jsonData)
      val data = metrics.flatMap(datum => {
        datum.dimension match {
          case CPU =>
            average(datum)
          case _ =>
            None
        }
      }).toSeq.toArray
      output(Message(write[Array[Datum]](data),msg.timestamp))
    }).failed.foreach(LOG.error("bad message", _))
  }
}

class MemoryProcessor(taskContext: TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) with Average {

  import taskContext.output

  val pipelineConfig = conf.getValue[PipelineConfig](PIPELINE)

  override def timeInterval = pipelineConfig.map(config => {
    config.getInt(MEM_INTERVAL)
  }).getOrElse(DEFAULT_INTERVAL)

  override def onStart(newStartTime: StartTime): Unit = {
    LOG.info("starting timeInterval=$timeInterval")
  }

  override def onNext(msg: Message): Unit = {
    Try({
      implicit val timeStamp = msg.timestamp
      val jsonData = msg.msg.asInstanceOf[String]
      val metrics = read[Array[Datum]](jsonData)
      val data = metrics.flatMap(datum => {
        datum.dimension match {
          case MEM =>
            average(datum)
          case _ =>
            None
        }
      }).toSeq.toArray
      output(Message(write[Array[Datum]](data),msg.timestamp))
    }).failed.foreach(LOG.error("bad message", _))
  }
}

class CpuPersistor(taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) with HBaseConsumer {

  def userConf = conf

  lazy val hbase = getHBase(table, hbaseConf)(userConf.getValue[HBaseRepo](HBASESINK).get)

  override def onStart(newStartTime: StartTime): Unit = {
    LOG.info("starting")
  }

  override def onNext(msg: Message): Unit = {
    Try({
      val cpus = read[Array[Datum]](msg.msg.asInstanceOf[String])
      cpus.foreach(cpu => {
        hbase.insert(msg.timestamp.toString, family, column, write[Datum](cpu))
      })
    }).failed.foreach(LOG.error("bad message", _))
  }

}

class MemoryPersistor(taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) with HBaseConsumer {

  def userConf = conf

  lazy val hbase = getHBase(table, hbaseConf)(userConf.getValue[HBaseRepo](HBASESINK).get)

  override def onStart(newStartTime: StartTime): Unit = {
    LOG.info("starting")
  }

  override def onNext(msg: Message): Unit = {
    Try({
      val memories = read[Array[Datum]](msg.msg.asInstanceOf[String])
      memories.foreach(memory => {
        hbase.insert(msg.timestamp.toString, family, column, write[Datum](memory))
      })
    }).failed.foreach(LOG.error("bad message", _))
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
      source.setStartTime(startTime)
    }).failed.foreach(LOG.error("caught error", _))
    self ! Message("start", System.currentTimeMillis())
  }

  override def onNext(msg: Message): Unit = {
    Try({
      source.pull(batchSize).foreach(msg => {
        val jsonData = msg.msg.asInstanceOf[String]
        val envelope = read[Envelope](jsonData)
        val body = read[Body](envelope.body)
        val metrics = body.metrics
        output(Message(write[Array[Datum]](metrics), msg.timestamp))
      })
    }).failed.foreach(LOG.error("caught error", _))
    self ! Message("continue", System.currentTimeMillis())
  }

  override def onStop(): Unit = {
    LOG.info("closing kafka source...")
    source.close()
  }
}

trait HBaseRepo extends java.io.Serializable {
  def getHBase(table:String, conf: Configuration): HBaseSinkInterface
}

trait HBaseConsumer {
  this: Task =>
  val ZOOKEEPER = "hbase.zookeeper.connect"
  val TABLE_NAME = "hbase.table.name"
  val COLUMN_FAMILY = "hbase.table.column.family"
  val COLUMN_NAME = "hbase.table.column.name"
  val HBASE_ZOOKEEPER = "hbase.zookeeper.quorum"
  val hbaseConf = new Configuration
  val pipelineConfig = userConf.getValue[PipelineConfig](PIPELINE)
  val (zookeepers, (table, family, column)) = pipelineConfig.map(config => {
    val zookeepers = config.getString(ZOOKEEPER)
    val table = config.getString(TABLE_NAME)
    val family = config.getString(COLUMN_FAMILY)
    val column = config.getString(COLUMN_NAME)
    (zookeepers, (table, family, column))
  }).get
  hbaseConf.set(HBASE_ZOOKEEPER, zookeepers)

  def userConf:UserConfig

  def getHBase(table:String, conf: Configuration) = scalaz.Reader((repo: HBaseRepo) => repo.getHBase(table, conf))

}




