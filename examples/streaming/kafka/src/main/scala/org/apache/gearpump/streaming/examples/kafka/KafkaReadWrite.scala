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

package org.apache.gearpump.streaming.examples.kafka

import java.util.Properties

import akka.actor.ActorSystem
import org.apache.gearpump.streaming.kafka.util.KafkaConfig
import org.slf4j.Logger

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.streaming.partitioner.ShufflePartitioner
import org.apache.gearpump.streaming.StreamApplication
import org.apache.gearpump.streaming.kafka._
import org.apache.gearpump.streaming.sink.DataSinkProcessor
import org.apache.gearpump.streaming.source.DataSourceProcessor
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{AkkaApp, Graph, LogUtil}

object KafkaReadWrite extends AkkaApp with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "source" -> CLIOption[Int]("<hom many kafka producer tasks>", required = false,
      defaultValue = Some(1)),
    "sink" -> CLIOption[Int]("<hom many kafka processor tasks>", required = false,
      defaultValue = Some(1)),
    "zookeeperConnect" -> CLIOption[String]("<zookeeper connect string>", required = false,
      defaultValue = Some("localhost:2181")),
    "brokerList" -> CLIOption[String]("<broker server list string>", required = false,
      defaultValue = Some("localhost:9092")),
    "sourceTopic" -> CLIOption[String]("<kafka source topic>", required = false,
      defaultValue = Some("topic1")),
    "sinkTopic" -> CLIOption[String]("<kafka sink topic>", required = false,
      defaultValue = Some("topic2"))
  )

  def application(config: ParseResult, system: ActorSystem): StreamApplication = {
    implicit val actorSystem = system
    val appName = "KafkaReadWrite"
    val sourceNum = config.getInt("source")
    val sinkNum = config.getInt("sink")
    val zookeeperConnect = config.getString("zookeeperConnect")
    val brokerList = config.getString("brokerList")
    val sourceTopic = config.getString("sourceTopic")
    val sinkTopic = config.getString("sinkTopic")

    val appConfig = UserConfig.empty
    val props = new Properties
    props.put(KafkaConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeperConnect)
    props.put(KafkaConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    props.put(KafkaConfig.CHECKPOINT_STORE_NAME_PREFIX_CONFIG, appName)
    val source = new KafkaSource(sourceTopic, props)
    val checkpointStoreFactory = new KafkaStoreFactory(props)
    source.setCheckpointStore(checkpointStoreFactory)
    val sourceProcessor = DataSourceProcessor(source, sourceNum)
    val sink = new KafkaSink(sinkTopic, props)
    val sinkProcessor = DataSinkProcessor(sink, sinkNum)
    val partitioner = new ShufflePartitioner
    val computation = sourceProcessor ~ partitioner ~> sinkProcessor
    val app = StreamApplication(appName, Graph(computation), appConfig)
    app
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    val context = ClientContext(akkaConf)
    val appId = context.submit(application(config, context.system))
    context.close()
  }
}
