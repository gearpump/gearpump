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

import com.julianpeeters.avro.annotations._
import com.typesafe.config.ConfigFactory
import org.apache.gearpump._
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.partitioner.ShufflePartitioner
import org.apache.gearpump.streaming.examples.kafka.KafkaStreamProducer
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}
import org.apache.gearpump.streaming.transaction.api.TimeReplayableSource
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{Constants, Graph, LogUtil}
import org.slf4j.Logger

import scala.util.{Failure, Success, Try}

case class SpaceShuttleMessage(id: String, on: String, body: String)

/**
 * This annotation will translate the case class into a Avro type class.
 * Fields must be vars in order to be compatible with the SpecificRecord API.
 * We will use this case class directly in ParquetWriterTask.
 */
@AvroRecord
case class SpaceShuttleRecord(var timestamp: Double, var vectorClass: Double, var count: Double)


class SpaceShuttleProducer(taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {

  class SpaceShuttleReplayableSource extends TimeReplayableSource {
    val data = Array[String](
      """
        |{"id":"2a329674-12ad-49f7-b40d-6485aae0aae8","on":"2015-04-02T18:52:02.680178753Z","body":"[-0.414141,-0.0246564,-0.125,0.0140301,-0.474359,0.0256049,-0.0980392,0.463884,0.40836]"}
      """
        .stripMargin,
      """
        |{"id":"043ade58-2fbc-4fe2-8253-84ab181b8cfa","on":"2015-04-02T18:52:02.680078434Z","body": "[-0.414141,-0.0246564,-0.125,0.0140301,-0.474359,0.0256049,-0.0980392,0.463884,0.40836]"}
      """.stripMargin,
      """
        |{"id":"043ade58-2fbc-4fe2-8253-84ab181b8cfa","on":"2015-04-02T18:52:02.680078434Z","body": "[-0.414141,-0.0246564,-0.125,0.0140301,-0.474359,0.0256049,-0.0980392,0.463884,0.40836]"}
      """.stripMargin
    )

    def startFromBeginning(): Unit = {}

    def setStartTime(startTime: TimeStamp): Unit = {}

    def pull(num: Int): List[Message] = List(Message(data(0)), Message(data(1)), Message(data(2)))

    def close(): Unit = {}
  }

  import taskContext.{output, parallelism}

  private val batchSize = 3

  val taskParallelism = parallelism

  private val source: TimeReplayableSource = new SpaceShuttleReplayableSource()
  private var startTime: TimeStamp = 0L

  override def onStart(newStartTime: StartTime): Unit = {
    startTime = newStartTime.startTime
    LOG.info(s"start time $startTime")
    source.setStartTime(startTime)
    self ! Message("start", System.currentTimeMillis())
  }

  override def onNext(msg: Message): Unit = {
    Try({

      source.pull(batchSize).foreach(msg => {
        output(msg)
      })
    }) match {
      case Success(ok) =>
      case Failure(throwable) =>
        LOG.error(s"failed ${throwable.getMessage}")
    }
    self ! Message("continue", System.currentTimeMillis())
  }

  override def onStop(): Unit = {
    LOG.info("closing kafka source...")
    source.close()
  }
}

object KafkaHdfsPipeLine extends App with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "reader"-> CLIOption[Int]("<kafka data reader number>", required = false, defaultValue = Some(2)),
    "scorer"-> CLIOption[Int]("<scorer number>", required = false, defaultValue = Some(2)),
    "writer"-> CLIOption[Int]("<parquet file writer number>", required = false, defaultValue = Some(1)),
    "output"-> CLIOption[String]("<output path directory>", required = false, defaultValue = Some("file:///tmp/parquet")),
    "conf" -> CLIOption[String]("<conf file>", required = false, defaultValue = Some("conf/kafka-hdfs-pipeline.conf"))
  )

  val context = ClientContext()
  implicit val system = context.system

  def application(context: ClientContext, config: ParseResult): Unit = {
    val readerNum = config.getInt("reader")
    val scorerNum = config.getInt("scorer")
    val writerNum = config.getInt("writer")
    val outputPath = config.getString("output")
    val pipeLinePath = config.getString("conf")
    val file = new java.io.File(pipeLinePath)
    val pipeLineConfig = file.exists() match {
      case true =>
        ConfigFactory.parseFile(file)
      case false =>
        val backup =
          """
            |gearpump {
            |  serializers {
            |    "org.apache.gearpump.examples.streaming.pipeline.SpaceShuttleRecord" = ""
            |  }
            |}
            |
            |kafka {
            |    consumer {
            |      # change to match cluster zookeepers
            |      zookeeper.connect = "10.10.10.46:2181,10.10.10.236:2181,10.10.10.164:2181/kafka"
            |      # consumer topics separated by ","
            |      topics = "topic-105"
            |      client.id = "kafka_hdfs_app"
            |      socket.timeout.ms = 30000
            |      socket.receive.buffer.bytes = 65536
            |      fetch.message.max.bytes = 1048576
            |      emit.batch.size = 100
            |      fetch.threshold = 5000
            |      fetch.sleep.ms = 100
            |    }
            |
            |    producer {
            |      topic = "topic-105"
            |      # list of host/port pairs to use for establishing the initial connection to kafka server
            |      # list should be in the form "host1:port1,host2:port2,..."
            |      # list need not contain the full set of servers
            |      bootstrap.servers = "10.10.10.46:9092,10.10.10.164:9092,10.10.10.236:9092"
            |      acks = "1"
            |      buffer.memory = 33554432
            |      compression.type = "none"
            |      retries = 0
            |      batch.size = 16384
            |    }
            |
            |    storage.replicas = 2
            |
            |    grouper.factory.class = "org.apache.gearpump.streaming.kafka.lib.grouper.KafkaDefaultGrouperFactory"
            |
            |    task {
            |      message.decoder.class = "org.apache.gearpump.streaming.kafka.lib.DefaultMessageDecoder"
            |      timestamp.filter.class = "org.apache.gearpump.streaming.kafka.lib.KafkaFilter"
            |    }
            |}
          """.stripMargin
        ConfigFactory.parseString(backup)
    }
    val kafkaConfig = KafkaConfig(pipeLineConfig)
    val appConfig = UserConfig.empty.withValue(KafkaConfig.NAME, kafkaConfig).withString(ParquetWriterTask.PARQUET_OUTPUT_DIRECTORY, outputPath)
    System.setProperty(Constants.GEARPUMP_CUSTOM_CONFIG_FILE, pipeLinePath)

    val partitioner = new ShufflePartitioner()
    val reader = Processor[KafkaStreamProducer](readerNum)
    //val reader = Processor[SpaceShuttleProducer](readerNum)
    val scorer = Processor[ScoringTask](scorerNum)
    val writer = Processor[ParquetWriterTask](writerNum)

    val dag = Graph(reader ~ partitioner ~> scorer ~ partitioner ~> writer)
    //val dag = Graph(reader ~ partitioner ~> scorer)
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
