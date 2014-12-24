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

import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.examples.kafka.topn.RollingTopWords._
import org.apache.gearpump.streaming.examples.kafka.{KafkaStreamProcessor, KafkaStreamProducer}
import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaConfig
import org.apache.gearpump.streaming.{AppMaster, AppDescription, TaskDescription}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{Configs, Graph}
import org.slf4j.{Logger, LoggerFactory}

object KafkaWordCount extends App with ArgumentsParser {
  private val LOG: Logger = LoggerFactory.getLogger(getClass())

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
    "kafka_stream_producer" -> CLIOption[Int]("<hom many kafka producer tasks>", required = false, defaultValue = Some(1)),
    "split" -> CLIOption[Int]("<how many split tasks>", required = false, defaultValue = Some(4)),
    "sum" -> CLIOption[Int]("<how many sum tasks>", required = false, defaultValue = Some(4)),
    "kafka_stream_processor" -> CLIOption[Int]("<hom many kafka processor tasks", required = false, defaultValue = Some(4)),
    "runseconds"-> CLIOption[Int]("<how long to run this example>", required = false, defaultValue = Some(60)))

  def application(config: ParseResult) : AppDescription = {
    val kafkaStreamProducerNum = config.getInt("kafka_stream_producer")
    val splitNum = config.getInt("split")
    val sumNum = config.getInt("sum")
    val kafkaStreamProcessorNum = config.getInt("kafka_stream_processor")
    val appConfig = Configs(KafkaConfig())
    val partitioner = new HashPartitioner()
    val kafkaStreamProducer = TaskDescription(classOf[KafkaStreamProducer].getCanonicalName, kafkaStreamProducerNum)
    val split = TaskDescription(classOf[Split].getCanonicalName, splitNum)
    val sum = TaskDescription(classOf[Sum].getCanonicalName, sumNum)
    val kafkaStreamProcessor = TaskDescription(classOf[KafkaStreamProcessor].getCanonicalName, kafkaStreamProcessorNum)
    val computation = kafkaStreamProducer ~ partitioner ~> split ~ partitioner ~> sum ~ partitioner ~> kafkaStreamProcessor
    val app = AppDescription("KafkaWordCount", classOf[AppMaster].getCanonicalName, appConfig, Graph(computation))
    app
  }

  val config = parse(args)
  val context = ClientContext(config.getString("master"))
  val appId = context.submit(application(config))
  Thread.sleep(config.getInt("runseconds") * 1000)
  context.shutdown(appId)
  context.cleanup()
}
