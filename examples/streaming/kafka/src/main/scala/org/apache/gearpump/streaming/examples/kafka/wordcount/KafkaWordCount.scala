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
import org.apache.gearpump.streaming.examples.kafka.{KafkaStreamProcessor, KafkaStreamProducer}
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.kafka.lib.consumer.KafkaConsumerConfig
import org.apache.gearpump.streaming.kafka.lib.producer.KafkaProducerConfig
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{Graph, LogUtil}
import org.slf4j.Logger

object KafkaWordCount extends App with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "kafka_stream_producer" -> CLIOption[Int]("<hom many kafka producer tasks>", required = false, defaultValue = Some(1)),
    "split" -> CLIOption[Int]("<how many split tasks>", required = false, defaultValue = Some(1)),
    "sum" -> CLIOption[Int]("<how many sum tasks>", required = false, defaultValue = Some(1)),
    "kafka_stream_processor" -> CLIOption[Int]("<hom many kafka processor tasks", required = false, defaultValue = Some(4))
    )

  def application(config: ParseResult) : StreamApplication = {
    val kafkaStreamProducerNum = config.getInt("kafka_stream_producer")
    val splitNum = config.getInt("split")
    val sumNum = config.getInt("sum")
    val kafkaStreamProcessorNum = config.getInt("kafka_stream_processor")

    val kafkaConfig = new KafkaConfig(
      (ConfigFactory.parseResources("kafka.conf")),
      KafkaConsumerConfig("consumer.properties"),
      KafkaProducerConfig("producer.properties"))
    val appConfig = UserConfig.empty
      .withValue(KafkaConfig.NAME, kafkaConfig)

    val kafkaStreamProducer = Processor[KafkaStreamProducer](kafkaStreamProducerNum)
    val split = Processor[Split](splitNum)
    val sum = Processor[Sum](sumNum)
    val kafkaStreamProcessor = Processor[KafkaStreamProcessor](kafkaStreamProcessorNum)
    val computation = kafkaStreamProducer ~> split ~> sum ~> kafkaStreamProcessor
    val app = StreamApplication("KafkaWordCount", Graph(computation), appConfig)
    app
  }

  val config = parse(args)
  val context = ClientContext()

  implicit val system = context.system

  val appId = context.submit(application(config))
  context.close()
}
