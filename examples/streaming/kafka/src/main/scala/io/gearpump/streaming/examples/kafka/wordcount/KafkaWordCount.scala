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

package io.gearpump.streaming.examples.kafka.wordcount

import akka.actor.ActorSystem
import io.gearpump.streaming.kafka.lib.KafkaSourceConfig
import io.gearpump.streaming.{StreamApplication, Processor}
import io.gearpump.streaming.kafka.{KafkaSink, KafkaStorageFactory, KafkaSource}
import io.gearpump.streaming.sink.DataSinkProcessor
import io.gearpump.streaming.source.DataSourceProcessor
import io.gearpump.cluster.UserConfig
import io.gearpump.cluster.client.ClientContext
import io.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import io.gearpump.partitioner.HashPartitioner
import io.gearpump.util.Graph._
import io.gearpump.util.{AkkaApp, Graph, LogUtil}
import kafka.api.OffsetRequest
import org.slf4j.Logger

object KafkaWordCount extends AkkaApp with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "source" -> CLIOption[Int]("<hom many kafka producer tasks>", required = false, defaultValue = Some(1)),
    "split" -> CLIOption[Int]("<how many split tasks>", required = false, defaultValue = Some(1)),
    "sum" -> CLIOption[Int]("<how many sum tasks>", required = false, defaultValue = Some(1)),
    "sink" -> CLIOption[Int]("<hom many kafka processor tasks", required = false, defaultValue = Some(1))
    )

  def application(config: ParseResult, system: ActorSystem) : StreamApplication = {
    implicit val actorSystem = system
    val sourceNum = config.getInt("source")
    val splitNum = config.getInt("split")
    val sumNum = config.getInt("sum")
    val sinkNum = config.getInt("sink")

    val appConfig = UserConfig.empty
    val offsetStorageFactory = new KafkaStorageFactory("localhost:2181", "localhost:9092")
    val kafkaSourceConfig = new KafkaSourceConfig()
      .withConsumerTopics("topic1").withConsumerStartOffset(OffsetRequest.LatestTime)
    val source = new KafkaSource(kafkaSourceConfig, offsetStorageFactory)
    val sourceProcessor = DataSourceProcessor(source, sourceNum)
    val split = Processor[Split](splitNum)
    val sum = Processor[Sum](sumNum)
    val sink = new KafkaSink("topic2", "localhost:9092")
    val sinkProcessor = DataSinkProcessor(sink, sinkNum)
    val partitioner = new HashPartitioner
    val computation = sourceProcessor ~ partitioner ~> split ~ partitioner ~> sum ~ partitioner ~> sinkProcessor
    val app = StreamApplication("KafkaWordCount", Graph(computation), appConfig)
    app
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    val context = ClientContext(akkaConf)
    val appId = context.submit(application(config, context.system))
    context.close()
  }
}
