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

package org.apache.gearpump.streaming.examples.kafka.wordcount

import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.kafka.sink.KafkaSink
import org.apache.gearpump.streaming.kafka.source.KafkaSource
import org.apache.gearpump.streaming.sink.DataSinkProcessor
import org.apache.gearpump.streaming.source.DataSourceProcessor
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{Graph, LogUtil}
import org.slf4j.Logger

object KafkaWordCount extends App with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "kafka_source" -> CLIOption[Int]("<hom many kafka source tasks>", required = false, defaultValue = Some(1)),
    "split" -> CLIOption[Int]("<how many split tasks>", required = false, defaultValue = Some(1)),
    "sum" -> CLIOption[Int]("<how many sum tasks>", required = false, defaultValue = Some(1)),
    "kafka_sink" -> CLIOption[Int]("<hom many kafka sink tasks>", required = false, defaultValue = Some(1))
    )

  def application(config: ParseResult) : StreamApplication = {
    val kafkaSourceNum = config.getInt("kafka_source")
    val splitNum = config.getInt("split")
    val sumNum = config.getInt("sum")
    val kafkaSinkNum = config.getInt("kafka_sink")

    val kafkaConfig = KafkaConfig(ConfigFactory.parseResources("kafka.conf"))

    val kafkaSource = new KafkaSource(kafkaConfig)
    val sourceProcessor = DataSourceProcessor(kafkaSource, kafkaSourceNum)
    val split = Processor[Split](splitNum)
    val sum = Processor[Sum](sumNum)
    val kafkaSink = new KafkaSink(kafkaConfig)
    val sinkProcessor = DataSinkProcessor(kafkaSink, kafkaSinkNum)
    val computation = sourceProcessor ~> split ~> sum ~> sinkProcessor
    val app = StreamApplication("KafkaWordCount", Graph(computation), UserConfig.empty)
    app
  }

  val config = parse(args)
  val context = ClientContext()

  implicit val system = context.system

  val appId = context.submit(application(config))
  context.close()
}
