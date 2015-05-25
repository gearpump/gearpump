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

package org.apache.gearpump.streaming.examples.kafka.topn

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.partitioner.{Partitioner, HashPartitioner}
import org.apache.gearpump.streaming.examples.kafka.KafkaStreamProducer
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.task.Task
import org.apache.gearpump.streaming.{Processor, StreamApplication, ProcessorDescription}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{Graph, LogUtil}
import org.slf4j.Logger

object RollingTopWords extends App with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "kafka_stream_producer" -> CLIOption[Int]("<hom many kafka producer tasks>", required = false, defaultValue = Some(1)),
    "rolling_count" -> CLIOption[Int]("<how many rolling count tasks>", required = false, defaultValue = Some(1)),
    "intermediate_ranker" -> CLIOption[Int]("<how many intermediate ranker tasks>", required = false, defaultValue = Some(1)))

  def application(config: ParseResult)(implicit system: ActorSystem) : StreamApplication = {
    val windowConfig = UserConfig(Map(
      Config.EMIT_FREQUENCY_MS ->  1000.toString,
      Config.WINDOW_LENGTH_MS -> 5000.toString,
      Config.TOPN -> 5.toString))

    val kafkaConfig = KafkaConfig(ConfigFactory.parseResources("kafka.conf"))
    val appConfig = windowConfig.withValue(KafkaConfig.NAME, kafkaConfig)

    val kafkaStreamProducerNum = config.getInt("kafka_stream_producer")
    val rcNum = config.getInt("rolling_count")
    val irNum = config.getInt("intermediate_ranker")

    val kafkaStreamProducer = Processor[KafkaStreamProducer](kafkaStreamProducerNum)
    val rollingCount = Processor[RollingCount](rcNum)
    val intermediateRanker = Processor[Ranker](irNum)
    val totalRanker = Processor[Ranker](1)
    val app = StreamApplication("RollingTopWords",
      Graph(kafkaStreamProducer ~> rollingCount
        ~> intermediateRanker ~> totalRanker), appConfig)
    app
  }

  val config = parse(args)
  val context = ClientContext()

  implicit val system = context.system

  val appId = context.submit(application(config))
  context.close()
}

object Config {
  val EMIT_FREQUENCY_MS = "emit.frequency.ms"
  val WINDOW_LENGTH_MS = "window.length.ms"
  val TOPN = "topn"

  def getEmitFrequencyMS(config: UserConfig) = config.getInt(EMIT_FREQUENCY_MS).get

  def getWindowLengthMS(config: UserConfig) = config.getInt(WINDOW_LENGTH_MS).get

  def getTopN(config: UserConfig) = config.getInt(TOPN).get
}