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

package org.apache.gearpump.experiments.kafka_hdfs_pipeline

import com.julianpeeters.avro.annotations._
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.partitioner.ShufflePartitioner
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.streaming.examples.kafka.KafkaStreamProducer
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.streaming.dsl.StreamApp
import org.apache.gearpump.streaming.dsl.StreamApp._
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.util.{Graph, Constants, LogUtil}
import org.slf4j.Logger
import upickle._

import scala.util.Try

case class SpaceShuttleMessage(id: String, on: String, body: String)

/**
 * This annotation will translate the case class into a Avro type class.
 * Fields must be vars in order to be compatible with the SpecificRecord API.
 * We will use this case class directly in ParquetWriterTask.
 */
@AvroRecord
case class SpaceShuttleRecord(var timestamp: Double, var vectorClass: Double, var count: Double)

//"http://atk-scoringengine.demo-gotapaas.com/v1/models/DemoModel/score?data=";

object PipeLine extends App with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  val PIPELINE = "pipeline"

  override val options: Array[(String, CLIOption[Any])] = Array(
    "reader"-> CLIOption[Int]("<kafaka data reader number>", required = false, defaultValue = Some(2)),
    "scorer"-> CLIOption[Int]("<scorer number>", required = false, defaultValue = Some(2)),
    "writer"-> CLIOption[Int]("<parquet file writer number>", required = false, defaultValue = Some(2)),
    "output"-> CLIOption[String]("<output path directory>", required = false, defaultValue = Some("file:///tmp/parquet")),
    "conf" -> CLIOption[String]("<conf file>", required = true)
  )

  val context = ClientContext()
  implicit val system = context.system

  def application(context: ClientContext, config: ParseResult): Unit = {
    val readerNum = config.getInt("reader")
    val scorerNum = config.getInt("scorer")
    val writerNum = config.getInt("writer")
    val outputPath = config.getString("output")
    val pipeLinePath = config.getString("conf")
    val pipeLineConfig = ConfigFactory.parseFile(new java.io.File(pipeLinePath))
    val kafkaConfig = KafkaConfig(pipeLineConfig)
    val appConfig = UserConfig.empty.withValue(KafkaConfig.NAME, kafkaConfig).withValue(PIPELINE, pipeLineConfig)
      .withString(ParquetWriterTask.PARQUET_OUTPUT_DIRECTORY, outputPath)
    System.setProperty(Constants.GEARPUMP_CUSTOM_CONFIG_FILE, pipeLinePath)

    val partitioner = new ShufflePartitioner()
    val reader = Processor[KafkaStreamProducer](readerNum)
    val scorer = Processor[ScoringTask](scorerNum)
    val writer = Processor[ParquetWriterTask](writerNum)

    val dag = Graph(reader ~ partitioner ~> scorer ~ partitioner ~> writer)
    val app = StreamApplication("PipeLine", dag, appConfig)

//    val app = StreamApp("PipeLine", context, appConfig)
//    app.readFromKafka(kafkaConfig, msg => {
//      val jsonData = msg.msg.asInstanceOf[String]
//      read[SpaceShuttleMessage](jsonData)
//    }, 1, "space-shuttle-data-producer").flatMap(spaceShuttleMessage => {
//      Some(upickle.read[Array[Float]](spaceShuttleMessage.body))
//    }).map(scoringVector => {
//
//    })

    context.submit(app)
    context.close()
  }

  Try({
    application(context, parse(args))
  }).failed.foreach(throwable => {
    LOG.error("Application Failed", throwable)
  })
}
