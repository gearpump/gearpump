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
package org.apache.gearpump.integrationtest.storm

import java.util.{ArrayList => JArrayList, HashMap => JHashMap, Map => JMap}

import backtype.storm.topology.TopologyBuilder
import backtype.storm.{Config, StormSubmitter}
import storm.kafka.bolt.KafkaBolt
import storm.kafka.{KafkaSpout, SpoutConfig, ZkHosts}

import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}

/**
 * Tests Storm 0.9.x compatibility over Gearpump
 * this example reads data from Kafka and writes back to it
 */
object Storm09KafkaTopology extends App with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array(
    "topologyName" -> CLIOption[Int]("<Storm topology name>", required = true),
    "sourceTopic" -> CLIOption[String]("<Kafka topic to read data>", required = true),
    "sinkTopic" -> CLIOption[String]("<Kafka topic to write data>", required = true),
    "zookeeperConnect" -> CLIOption[String]("<Zookeeper connect string, e.g. localhost:2181/kafka>",
      required = true),
    "brokerList" -> CLIOption[String]("<Kafka broker list, e.g. localhost:9092>", required = true),
    "spoutNum" -> CLIOption[Int]("<how many spout tasks>", required = false,
      defaultValue = Some(1)),
    "boltNum" -> CLIOption[Int]("<how many bolt tasks>", required = false, defaultValue = Some(1))
  )

  val configs = parse(args)
  val topologyName = configs.getString("topologyName")
  val sourceTopic = configs.getString("sourceTopic")
  val sinkTopic = configs.getString("sinkTopic")
  val zookeeperConnect = configs.getString("zookeeperConnect")
  val brokerList = configs.getString("brokerList")
  val spoutNum = configs.getInt("spoutNum")
  val boltNum = configs.getInt("boltNum")

  val topologyBuilder = new TopologyBuilder()
  val kafkaSpout: KafkaSpout = new KafkaSpout(getSpoutConfig(sourceTopic, zookeeperConnect,
    topologyName))
  val kafkaBolt: KafkaBolt[Array[Byte], Array[Byte]] = new KafkaBolt[Array[Byte], Array[Byte]]()
  val adaptor = new Adaptor
  topologyBuilder.setSpout("kafka_spout", kafkaSpout, spoutNum)
  topologyBuilder.setBolt("adaptor", adaptor).localOrShuffleGrouping("kafka_spout")
  topologyBuilder.setBolt("kafka_bolt", kafkaBolt, boltNum).localOrShuffleGrouping("adaptor")
  val config = new Config()
  config.putAll(getBoltConfig(sinkTopic, brokerList))
  config.put(Config.TOPOLOGY_NAME, topologyName)
  val topology = topologyBuilder.createTopology()
  StormSubmitter.submitTopology(topologyName, config, topology)

  def getSpoutConfig(topic: String, zookeeperConnect: String, id: String): SpoutConfig = {
    val hosts = new ZkHosts(zookeeperConnect)
    val index = zookeeperConnect.indexOf("/")
    val zookeeper = zookeeperConnect.take(index)
    val kafkaRoot = zookeeperConnect.drop(index)
    val config = new SpoutConfig(hosts, topic, kafkaRoot, id)
    config.forceFromStart = true

    val serverAndPort = zookeeper.split(":")
    config.zkServers = new JArrayList[String]
    config.zkServers.add(serverAndPort(0))
    config.zkPort = serverAndPort(1).toInt
    config
  }

  def getBoltConfig(topic: String, brokerList: String): JMap[String, AnyRef] = {
    val kafkaConfig: JMap[String, AnyRef] = new JHashMap[String, AnyRef]
    val brokerConfig: JMap[String, AnyRef] = new JHashMap[String, AnyRef]
    brokerConfig.put("metadata.broker.list", brokerList)
    brokerConfig.put("request.required.acks", "1")
    kafkaConfig.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, brokerConfig)
    kafkaConfig.put(KafkaBolt.TOPIC, topic)
    kafkaConfig
  }
}

