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

import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult, Starter}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.examples.kafka.KafkaStreamProducer
import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaConfig
import org.apache.gearpump.streaming.{AppMaster, AppDescription, TaskDescription}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{Configs, Graph}
import org.slf4j.{Logger, LoggerFactory}

class RollingTopWords extends Starter with ArgumentsParser {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[RollingTopWords])

  import org.apache.gearpump.streaming.examples.kafka.topn.RollingTopWords._

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
    "kafka_stream_producer" -> CLIOption[Int]("<hom many kafka producer tasks>", required = false, defaultValue = Some(1)),
    "rolling_count" -> CLIOption[Int]("<how many rolling count tasks>", required = false, defaultValue = Some(4)),
    "intermediate_ranker" -> CLIOption[Int]("<how many intermediate ranker tasks>", required = false, defaultValue = Some(4)),
    "runseconds" -> CLIOption[Int]("<how long to run this example>", required = false, defaultValue = Some(60)))

  override def application(config: ParseResult) : AppDescription = {
    val windowConfig = Map(
      Config.EMIT_FREQUENCY_MS -> 1000,
      Config.WINDOW_LENGTH_MS -> 5000,
      Config.TOPN -> 5)
    val appConfig = Configs(windowConfig ++ KafkaConfig())
    val kafkaStreamProducerNum = config.getInt("kafka_stream_producer")
    val rcNum = config.getInt("rolling_count")
    val irNum = config.getInt("intermediate_ranker")
    val partitioner = new HashPartitioner()
    val kafkaStreamProducer = TaskDescription(classOf[KafkaStreamProducer].getCanonicalName, kafkaStreamProducerNum)
    val rollingCount = TaskDescription(classOf[RollingCount].getCanonicalName, rcNum)
    val intermediateRanker = TaskDescription(classOf[Ranker].getCanonicalName, irNum)
    val totalRanker = TaskDescription(classOf[Ranker].getCanonicalName, 1)
    val app = AppDescription("RollingTopWords", classOf[AppMaster], appConfig,
      Graph(kafkaStreamProducer ~ partitioner ~> rollingCount ~ partitioner
        ~> intermediateRanker ~ partitioner ~> totalRanker)
    )
    app
  }

}

object RollingTopWords {
  object Config {
    val EMIT_FREQUENCY_MS = "emit.frequency.ms"
    val WINDOW_LENGTH_MS = "window.length.ms"
    val TOPN = "topn"

    def getEmitFrequencyMS(config: Map[String, _]) = config.get(EMIT_FREQUENCY_MS).get.asInstanceOf[Int]
    def getWindowLengthMS(config: Map[String, _]) = config.get(WINDOW_LENGTH_MS).get.asInstanceOf[Int]
    def getTopN(config: Map[String, _]) = config.get(TOPN).get.asInstanceOf[Int]
  }
}
