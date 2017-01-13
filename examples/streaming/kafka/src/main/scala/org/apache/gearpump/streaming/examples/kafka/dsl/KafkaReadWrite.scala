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

package org.apache.gearpump.streaming.examples.kafka.dsl

import java.util.Properties

import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.streaming.dsl.scalaapi.StreamApp
import org.apache.gearpump.streaming.kafka.KafkaStoreFactory
import org.apache.gearpump.streaming.kafka.dsl.KafkaDSL
import org.apache.gearpump.streaming.kafka.dsl.KafkaDSL._
import org.apache.gearpump.streaming.kafka.util.KafkaConfig
import org.apache.gearpump.util.AkkaApp

object KafkaReadWrite extends AkkaApp with ArgumentsParser {

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
      defaultValue = Some("topic2")),
    "atLeastOnce" -> CLIOption[Boolean]("<turn on at least once source>", required = false,
      defaultValue = Some(true))
  )

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    val sourceNum = config.getInt("source")
    val sinkNum = config.getInt("sink")
    val zookeeperConnect = config.getString("zookeeperConnect")
    val brokerList = config.getString("brokerList")
    val sourceTopic = config.getString("sourceTopic")
    val sinkTopic = config.getString("sinkTopic")
    val atLeastOnce = config.getBoolean("atLeastOnce")
    val props = new Properties
    val appName = "KafkaDSL"
    props.put(KafkaConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeperConnect)
    props.put(KafkaConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    props.put(KafkaConfig.CHECKPOINT_STORE_NAME_PREFIX_CONFIG, appName)

    val context = ClientContext(akkaConf)
    val app = StreamApp(appName, context)

    if (atLeastOnce) {
      val checkpointStoreFactory = new KafkaStoreFactory(props)
      KafkaDSL.createAtLeastOnceStream(app, sourceTopic, checkpointStoreFactory, props, sourceNum)
        .writeToKafka(sinkTopic, props, sinkNum)
    } else {
      KafkaDSL.createAtMostOnceStream(app, sourceTopic, props, sourceNum)
        .writeToKafka(sinkTopic, props, sinkNum)
    }

    context.submit(app)
    context.close()
  }
}
