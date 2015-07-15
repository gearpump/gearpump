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

package org.apache.gearpump.examples.streaming.pipeline

import java.util.Properties

import com.julianpeeters.avro.annotations._
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.partitioner.ShufflePartitioner
import org.apache.gearpump.streaming.kafka.KafkaSource
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig._
import org.apache.gearpump.streaming.source.DataSourceProcessor
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{Graph, LogUtil}
import org.slf4j.Logger

import scala.util.Try

case class SpaceShuttleMessage(id: String, on: String, body: String)

/**
 * This annotation will translate the case class into a Avro type class.
 * Fields must be vars in order to be compatible with the SpecificRecord API.
 * We will use this case class directly in ParquetWriterTask.
 */
@AvroRecord
case class SpaceShuttleRecord(var timestamp: Double, var vectorClass: Double, var count: Double)

object KafkaHdfsPipeLine extends App with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "reader"-> CLIOption[Int]("<kafka data reader number>", required = false, defaultValue = Some(2)),
    "scorer"-> CLIOption[Int]("<scorer number>", required = false, defaultValue = Some(2)),
    "writer"-> CLIOption[Int]("<parquet file writer number>", required = false, defaultValue = Some(1)),
    "output"-> CLIOption[String]("<output path directory>", required = false, defaultValue = Some("file:///tmp/parquet")),
    "topic" -> CLIOption[String]("<topic>", required = false, defaultValue = Some("topic-105")),
    "brokers" -> CLIOption[String]("<brokers>", required = false, defaultValue = Some("10.10.10.46:9092,10.10.10.164:9092,10.10.10.236:9092")),
    "zookeepers" -> CLIOption[String]("<zookeepers>", required = false, defaultValue = Some("10.10.10.46:2181,10.10.10.236:2181,10.10.10.164:2181/kafka"))
  )

  val context = ClientContext()
  implicit val system = context.system

  def application(context: ClientContext, config: ParseResult): Unit = {
    val readerNum = config.getInt("reader")
    val scorerNum = config.getInt("scorer")
    val writerNum = config.getInt("writer")
    val outputPath = config.getString("output")
    val topic = config.getString("topic")
    val brokers = config.getString("brokers")
    val zookeepers = config.getString("zookeepers")
    val appConfig = UserConfig.empty.withString(ParquetWriterTask.PARQUET_OUTPUT_DIRECTORY, outputPath)
    val consumerProperties = new Properties()
    consumerProperties.setProperty(ZOOKEEPER_CONNECT, zookeepers)
    val producerProperties = new Properties()
    producerProperties.setProperty(BOOTSTRAP_SERVERS, brokers)
    val kafkaConfig = new KafkaConfig(
      consumerConfig=consumerProperties,
      producerConfig=producerProperties
    ).withConsumerTopics(topic)

    val partitioner = new ShufflePartitioner()
    val source = new KafkaSource(kafkaConfig)
    val reader = DataSourceProcessor(source, readerNum)
    val scorer = Processor[ScoringTask](scorerNum)
    val writer = Processor[ParquetWriterTask](writerNum)

    val dag = Graph(reader ~ partitioner ~> scorer ~ partitioner ~> writer)
    val app = StreamApplication("KafkaHdfsPipeLine", dag, appConfig)

    context.submit(app)
    context.close()
  }

  Try({
    application(context, parse(args))
  }).failed.foreach(throwable => {
    LOG.error("Application Failed", throwable)
  })
}
