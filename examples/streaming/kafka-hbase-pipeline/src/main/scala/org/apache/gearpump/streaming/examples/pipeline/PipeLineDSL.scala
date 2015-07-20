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
import org.apache.gearpump.external.hbase.HBaseSink
import org.apache.gearpump.external.hbase.dsl.HBaseDSLSink._
import org.apache.gearpump.streaming.dsl.StreamApp
import org.apache.gearpump.streaming.dsl.StreamApp._
import org.apache.gearpump.streaming.examples.pipeline.Messages.{Body, Datum, Envelope, _}
import org.apache.gearpump.streaming.kafka.KafkaStorageFactory
import org.apache.gearpump.streaming.kafka.dsl.KafkaDSLUtil
import org.apache.gearpump.streaming.kafka.lib.{KafkaSourceConfig, KafkaUtil, StringMessageDecoder}
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.streaming.transaction.api.TimeReplayableSource
import org.apache.gearpump.util.{AkkaApp, Constants, LogUtil}
import org.slf4j.Logger
import upickle._

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
    val appConfig = UserConfig.empty.withValue(PIPELINE, pipeLineConfig)
    System.setProperty(Constants.GEARPUMP_CUSTOM_CONFIG_FILE, pipeLinePath)

    val tableName = pipeLineConfig.getString(HBaseSink.TABLE_NAME)
    val columnFamily = pipeLineConfig.getString(HBaseSink.COLUMN_FAMILY)
    val columnName = pipeLineConfig.getString(HBaseSink.COLUMN_NAME)

    val app = StreamApp("PipeLineDSL", context, appConfig)
    val topics = pipeLineConfig.getString("kafka.consumer.topics")
    val kafkaConfig = new KafkaSourceConfig(KafkaUtil.buildConsumerConfig("localhost:2181")).withConsumerTopics(topics)
    val offsetStorageFactory = new KafkaStorageFactory("localhost:2181", "localhost:9092")
    val kafkaStream = KafkaDSLUtil.createStream[String](app, 1, "time-replayable-producer",
      kafkaConfig, offsetStorageFactory, new StringMessageDecoder)
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
          val result = average.average(datum, now)
          result.map(cpu => (System.currentTimeMillis.toString, columnFamily, columnName, write[Datum](cpu)))
        })
      }
    })()).writeToHbase(tableName, 1, "sink")
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
          val result = average.average(datum, now)
          result.map(mem => (System.currentTimeMillis.toString, columnFamily, columnName, write[Datum](mem)))
        })
      }
    })()).writeToHbase(tableName, 1, "sink")

    context.submit(app)
    context.close()
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val context = ClientContext(akkaConf)
    application(context, parse(args))
  }
}
