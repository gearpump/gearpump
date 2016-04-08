/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.streaming.examples.state

import akka.actor.ActorSystem
import org.apache.hadoop.conf.Configuration

import io.gearpump.cluster.UserConfig
import io.gearpump.cluster.client.ClientContext
import io.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import io.gearpump.partitioner.HashPartitioner
import io.gearpump.streaming.examples.state.processor.CountProcessor
import io.gearpump.streaming.hadoop.HadoopCheckpointStoreFactory
import io.gearpump.streaming.hadoop.lib.rotation.FileSizeRotation
import io.gearpump.streaming.kafka.{KafkaSink, KafkaSource, KafkaStorageFactory}
import io.gearpump.streaming.sink.DataSinkProcessor
import io.gearpump.streaming.source.DataSourceProcessor
import io.gearpump.streaming.state.impl.PersistentStateConfig
import io.gearpump.streaming.{Processor, StreamApplication}
import io.gearpump.util.Graph.Node
import io.gearpump.util.{AkkaApp, Graph}

object MessageCountApp extends AkkaApp with ArgumentsParser {
  val SOURCE_TASK = "sourceTask"
  val COUNT_TASK = "countTask"
  val SINK_TASK = "sinkTask"
  val SOURCE_TOPIC = "sourceTopic"
  val SINK_TOPIC = "sinkTopic"
  val ZOOKEEPER_CONNECT = "zookeeperConnect"
  val BROKER_LIST = "brokerList"
  val DEFAULT_FS = "defaultFS"

  override val options: Array[(String, CLIOption[Any])] = Array(
    SOURCE_TASK -> CLIOption[Int]("<how many kafka source tasks>", required = false,
    defaultValue = Some(1)),
    COUNT_TASK -> CLIOption("<how many count tasks>", required = false, defaultValue = Some(1)),
    SINK_TASK -> CLIOption[Int]("<how many kafka sink tasks>", required = false,
    defaultValue = Some(1)),
    SOURCE_TOPIC -> CLIOption[String]("<kafka source topic>", required = true),
    SINK_TOPIC -> CLIOption[String]("<kafka sink topic>", required = true),
    ZOOKEEPER_CONNECT -> CLIOption[String]("<Zookeeper connect string, e.g. localhost:2181/kafka>",
    required = true),
    BROKER_LIST -> CLIOption[String]("<Kafka broker list, e.g. localhost:9092>", required = true),
    DEFAULT_FS -> CLIOption[String]("<name of the default file system, e.g. hdfs://localhost:9000>",
      required = true)
  )

  def application(config: ParseResult)(implicit system: ActorSystem): StreamApplication = {
    val hadoopConfig = new Configuration
    hadoopConfig.set("fs.defaultFS", config.getString(DEFAULT_FS))
    val checkpointStoreFactory = new HadoopCheckpointStoreFactory("MessageCount", hadoopConfig,
      // rotate on 1KB
      new FileSizeRotation(1000))
    val taskConfig = UserConfig.empty
      .withBoolean(PersistentStateConfig.STATE_CHECKPOINT_ENABLE, true)
      .withLong(PersistentStateConfig.STATE_CHECKPOINT_INTERVAL_MS, 1000L)
      .withValue(PersistentStateConfig.STATE_CHECKPOINT_STORE_FACTORY, checkpointStoreFactory)

    val zookeeperConnect = config.getString(ZOOKEEPER_CONNECT)
    val brokerList = config.getString(BROKER_LIST)
    val offsetStorageFactory = new KafkaStorageFactory(zookeeperConnect, brokerList)
    val sourceTopic = config.getString(SOURCE_TOPIC)
    val kafkaSource = new KafkaSource(sourceTopic, zookeeperConnect, offsetStorageFactory)
    val sourceProcessor = DataSourceProcessor(kafkaSource, config.getInt(SOURCE_TASK))
    val countProcessor = Processor[CountProcessor](config.getInt(COUNT_TASK), taskConf = taskConfig)
    val kafkaSink = new KafkaSink(config.getString(SINK_TOPIC), brokerList)
    val sinkProcessor = DataSinkProcessor(kafkaSink, config.getInt(SINK_TASK))
    val partitioner = new HashPartitioner()
    val graph = Graph(sourceProcessor ~ partitioner
      ~> countProcessor ~ partitioner ~> sinkProcessor)
    val app = StreamApplication("MessageCount", graph, UserConfig.empty)
    app
  }

  def main(akkaConf: Config, args: Array[String]): Unit = {

    val config = parse(args)
    val context = ClientContext(akkaConf)
    implicit val system = context.system
    val appId = context.submit(application(config))
    context.close()
  }
}
