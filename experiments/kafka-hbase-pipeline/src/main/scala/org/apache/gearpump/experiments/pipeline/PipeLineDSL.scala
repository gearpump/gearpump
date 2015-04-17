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

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.experiments.hbase.HBaseSink._
import org.apache.gearpump.experiments.hbase._
import org.apache.gearpump.experiments.pipeline.Messages._
import org.apache.gearpump.streaming.dsl.Stream._
import org.apache.gearpump.streaming.dsl.StreamApp
import org.apache.gearpump.streaming.dsl.StreamApp._
import org.apache.gearpump.streaming.dsl.op.OpType.Traverse
import org.apache.gearpump.streaming.kafka.KafkaSource
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.task.{TaskContext, TaskId}
import org.apache.gearpump.streaming.transaction.api.MessageDecoder
import org.apache.gearpump.util.{Constants, LogUtil}
import org.apache.hadoop.conf.Configuration
import org.slf4j.Logger
import upickle._

import scala.util.Try

class ReplayableSource(kafkaConfig: KafkaConfig) extends Traverse[Array[Datum]] {
  private val batchSize = kafkaConfig.getConsumerEmitBatchSize
  private val msgDecoder: MessageDecoder = kafkaConfig.getMessageDecoder
  lazy val source = new KafkaSource(kafkaConfig.getClientId, TaskId(0,0), 1, kafkaConfig, msgDecoder)
  override def foreach[U](fun: Array[Datum] => U): Unit = {
    val list = source.pull(batchSize)
    list.foreach(msg => {
      val jsonData = msg.msg.asInstanceOf[String]
      val envelope = read[Envelope](jsonData)
      val body = read[Body](envelope.body)
      val metrics = body.metrics
      fun(metrics)
    })
  }
}

object PipeLineDSL extends App with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  val PROCESSORS = "pipeline.processors"
  val PERSISTORS = "pipeline.persistors"

  override val options: Array[(String, CLIOption[Any])] = Array(
    "conf" -> CLIOption[String]("<conf file>", required = true)
  )
  val context = ClientContext()
  implicit val system = context.system

  def toHBaseSink(taskContext: TaskContext, userConfig: UserConfig): Array[Datum] => Unit = {
    val LOG: Logger = LogUtil.getLogger(taskContext.getClass)
    val repo = new HBaseRepo {
      def getHBase(table: String, conf: Configuration): HBaseSinkInterface = HBaseSink(table, conf)
    }
    val config: Config = userConfig.getValue[PipeLineConfig](PIPELINE).get
    val hbaseConsumer = HBaseConsumer(taskContext.system, Some(config))
    def hbaseSink(hbase: HBaseSinkInterface)(metrics: Array[Datum]): Unit = {
      LOG.info("success")
      metrics.foreach(datum => {
        hbase.insert(System.currentTimeMillis.toString, hbaseConsumer.table, hbaseConsumer.table, datum.value.toString)
      })
    }
    hbaseSink(hbaseConsumer.getHBase(repo))
  }

  def application(context: ClientContext, config: ParseResult): Unit = {
    val pipeLinePath = config.getString("conf")
    val pipeLineConfig = PipeLineConfig(ConfigFactory.parseFile(new java.io.File(pipeLinePath)))
    val persistors = pipeLineConfig.getInt(PERSISTORS)
    val kafkaConfig = KafkaConfig(pipeLineConfig)
    val repo = new HBaseRepo {
      def getHBase(table: String, conf: Configuration): HBaseSinkInterface = HBaseSink(table, conf)
    }
    val appConfig = UserConfig.empty.withValue(KafkaConfig.NAME, kafkaConfig).withValue(PIPELINE, pipeLineConfig).withValue(HBASESINK, repo)
    System.setProperty(Constants.GEARPUMP_CUSTOM_CONFIG_FILE, pipeLinePath)

    val app = StreamApp("PipeLineDSL", context, appConfig)
    val producer = app.readFromKafka(new ReplayableSource(kafkaConfig), 1, "time-replayable-producer")

    producer.flatMap(metrics => {
      Some(metrics.flatMap(datum => {
        datum.dimension match {
          case CPU =>
            Some(datum)
          case _ =>
            None
        }
      }))
    }).reduce((msg1, msg2) => {
      val now = System.currentTimeMillis
      def average(average: TAverage)(metrics: Array[Datum]): Array[Datum] = {
        metrics.flatMap(datum => {
          average.average(datum, now)
        })
      }
      average(TAverage(50))(msg2)
    }).writeToHBase(toHBaseSink)

    producer.flatMap(metrics => {
      Some(metrics.flatMap(datum => {
        datum.dimension match {
          case MEM =>
            Some(datum)
          case _ =>
            None
        }
      }))
    }).reduce((msg1, msg2) => {
      val now = System.currentTimeMillis
      def average(average: TAverage)(metrics: Array[Datum]): Array[Datum] = {
        metrics.flatMap(datum => {
          average.average(datum, now)
        })
      }
      average(TAverage(100))(msg2)
    }).writeToHBase(toHBaseSink)

    /*
    val app = StreamApp("dsl", context)
    val input = "This is a good start, bingo!! bingo!!"
    app.source(input.lines.toList, 1).
      // word => (word, count)
      flatMap(line => line.split("[\\s]+")).map((_, 1)).
      // (word, count1), (word, count2) => (word, count1 + count2)
      groupByKey().sum.log
    */

    context.submit(app)
    context.close()

  }
  Try({
    application(context, parse(args))
  }).failed.foreach(throwable => {
    LOG.error("Application Failed", throwable)
  })
}
