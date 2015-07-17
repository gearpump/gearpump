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

package org.apache.gearpump.streaming.examples.pipeline

import com.typesafe.config.ConfigFactory
import org.apache.gearpump._
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import Messages.{Datum, Body, Envelope}
import org.apache.gearpump.external.hbase.{HBaseConsumer, HBaseRepo, HBaseSinkInterface, HBaseSink}
import Messages._
import org.apache.gearpump.streaming.dsl.StreamApp
import org.apache.gearpump.streaming.dsl.StreamApp._
import org.apache.gearpump.streaming.kafka.dsl.KafkaDSLUtil
import org.apache.gearpump.streaming.kafka.lib.{StringMessageDecoder, KafkaConfig}
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.streaming.transaction.api.TimeReplayableSource
import org.apache.gearpump.util.{AkkaApp, LogUtil}
import org.apache.hadoop.conf.Configuration
import org.slf4j.Logger
import upickle._

import scala.util.Try
import org.apache.gearpump.external.hbase.HBaseSink._
import org.apache.gearpump.util.Constants

class TimeReplayableSourceTest1 extends TimeReplayableSource {
  val data = Array[String](
    """
      |{"id":"2a329674-12ad-49f7-b40d-6485aae0aae8","on":"2015-04-02T18:52:02.680178753Z","body":"{\"sample_id\":\"sample-0\",\"source_id\":\"src-13\",\"event_ts\":\"2015-04-02T18:52:04.784086993Z\",\"metrics\":[{\"dimension\":\"CPU\",\"metric\":\"total\",\"value\":27993997},{\"dimension\":\"CPU\",\"metric\":\"user\",\"value\":39018},{\"dimension\":\"CPU\",\"metric\":\"sys\",\"value\":23299},{\"dimension\":\"LOAD\",\"metric\":\"min\",\"value\":0},{\"dimension\":\"MEM\",\"metric\":\"free\",\"value\":15607009280},{\"dimension\":\"MEM\",\"metric\":\"used\",\"value\":163528704}]}"}
    """.stripMargin,
    """
      |{"id":"043ade58-2fbc-4fe2-8253-84ab181b8cfa","on":"2015-04-02T18:52:02.680078434Z","body":"{\"sample_id\":\"sample-0\",\"source_id\":\"src-4\",\"event_ts\":\"2015-04-02T18:52:04.78364878Z\",\"metrics\":[{\"dimension\":\"CPU\",\"metric\":\"total\",\"value\":27993996},{\"dimension\":\"CPU\",\"metric\":\"user\",\"value\":39017},{\"dimension\":\"CPU\",\"metric\":\"sys\",\"value\":23299},{\"dimension\":\"LOAD\",\"metric\":\"min\",\"value\":0},{\"dimension\":\"MEM\",\"metric\":\"free\",\"value\":15607009280},{\"dimension\":\"MEM\",\"metric\":\"used\",\"value\":163528704}]}"}
    """.stripMargin,
    """
      |{"id":"043ade58-2fbc-4fe2-8253-84ab181b8cfa","on":"2015-04-02T18:52:02.680078434Z","body":"{\"sample_id\":\"sample-0\",\"source_id\":\"src-4\",\"event_ts\":\"2015-04-02T18:52:04.78364878Z\",\"metrics\":[{\"dimension\":\"CPU\",\"metric\":\"total\",\"value\":27993996},{\"dimension\":\"CPU\",\"metric\":\"user\",\"value\":39017},{\"dimension\":\"CPU\",\"metric\":\"sys\",\"value\":23299},{\"dimension\":\"LOAD\",\"metric\":\"min\",\"value\":0},{\"dimension\":\"MEM\",\"metric\":\"free\",\"value\":15607009280},{\"dimension\":\"MEM\",\"metric\":\"used\",\"value\":163528704}]}"}
    """.stripMargin
  )

  override def open(context: TaskContext, startTime: Option[TimeStamp]): Unit = {}

  override def read(batchSize: Int): List[Message] = List(Message(data(0)), Message(data(1)), Message(data(2)))

  override def close(): Unit = {}
}

object PipeLineDSL extends AkkaApp with ArgumentsParser {

  private val LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "conf" -> CLIOption[String]("<conf file>", required = true)
  )

  def application(context: ClientContext, config: ParseResult): Unit = {

    implicit val system = context.system

    val pipeLinePath = config.getString("conf")
    val pipeLineConfig = ConfigFactory.parseFile(new java.io.File(pipeLinePath))
    val kafkaConfig = new KafkaConfig(pipeLineConfig)
    val repo = new HBaseRepo {
      def getHBase(table: String, conf: Configuration): HBaseSinkInterface = HBaseSink(table, conf)
    }
    val appConfig = UserConfig.empty.withValue(KafkaConfig.NAME, kafkaConfig).withValue(PIPELINE, pipeLineConfig).withValue(HBASESINK, repo)
    System.setProperty(Constants.GEARPUMP_CUSTOM_CONFIG_FILE, pipeLinePath)

    val app = StreamApp("PipeLineDSL", context, appConfig)
    val kafkaStream = KafkaDSLUtil.createStream[String](app, 1, "time-replayable-producer", kafkaConfig, new StringMessageDecoder)
    val producer = kafkaStream.map{ message =>
      val envelope = read[Envelope](message)
      val body = read[Body](envelope.body)
      body.metrics
    }
    producer.flatMap(metrics => {
      Some(metrics.flatMap(datum => {
        datum.dimension match {
          case CPU =>
            Some(datum)
          case _ =>
            None
        }
      }))
    }).map((() => {
      val average = TAverage(pipeLineConfig.getInt(CPU_INTERVAL))
      msg: Array[Datum] => {
        val now = System.currentTimeMillis
        msg.flatMap(datum => {
          average.average(datum, now)
        })
      }
    })()).writeToHBase(pipeLineConfig, (sinkInterface:HBaseSinkInterface, hbaseConsumer:HBaseConsumer) => {
      metrics:Array[Datum] => {
        metrics.foreach(datum => {
          sinkInterface.insert(System.currentTimeMillis.toString, hbaseConsumer.family, hbaseConsumer.column, write[Datum](datum))
        })
      }
    })
    producer.flatMap(metrics => {
      Some(metrics.flatMap(datum => {
        datum.dimension match {
          case MEM =>
            Some(datum)
          case _ =>
            None
        }
      }))
    }).map((() => {
      val average = TAverage(pipeLineConfig.getInt(MEM_INTERVAL))
      msg: Array[Datum] => {
        val now = System.currentTimeMillis
        msg.flatMap(datum => {
          average.average(datum, now)
        })
      }
    })()).writeToHBase(pipeLineConfig, (sinkInterface:HBaseSinkInterface, hbaseConsumer:HBaseConsumer) => {
      metrics:Array[Datum] => {
        metrics.foreach(datum => {
          sinkInterface.insert(System.currentTimeMillis.toString, hbaseConsumer.family, hbaseConsumer.column, write[Datum](datum))
        })
      }
    })

    context.submit(app)
    context.close()
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val context = ClientContext(akkaConf)
    application(context, parse(args))
  }
}
