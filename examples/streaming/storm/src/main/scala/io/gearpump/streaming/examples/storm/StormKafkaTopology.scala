/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gearpump.streaming.examples.storm

import java.util.{ArrayList => JArrayList, HashMap => JHashMap, Map => JMap}

import backtype.storm.{StormSubmitter, Config}
import backtype.storm.topology.TopologyBuilder
import storm.kafka.{ZkHosts, SpoutConfig, KafkaSpout}
import storm.kafka.bolt.KafkaBolt

object StormKafkaTopology extends App {
  if (args.length < 7) {
    val usage =
      """
        | Usage:
        |    StormKafkaTopology topologyName spoutNum boltNum
        |         kafkaSpoutTopic kafkaBoltTopic zookeeperConnect brokerList
      """.stripMargin
    println(usage)
    throw new IllegalArgumentException
  }

  val topologyName = args(0)
  val spoutNum = args(1).toInt
  val boltNum = args(2).toInt
  val spoutTopic = args(3)
  val boltTopic = args(4)
  val zookeeperConnect = args(5)
  val brokerList = args(6)

  val topologyBuilder = new TopologyBuilder()
  val kafkaSpout: KafkaSpout = new KafkaSpout(getSpoutConfig(spoutTopic, zookeeperConnect, topologyName))
  val kafkaBolt: KafkaBolt[Array[Byte], Array[Byte]] = new KafkaBolt[Array[Byte], Array[Byte]]()
  val adaptor = new Adaptor
  topologyBuilder.setSpout("kafka_spout", kafkaSpout, spoutNum)
  topologyBuilder.setBolt("adaptor", adaptor).localOrShuffleGrouping("kafka_spout")
  topologyBuilder.setBolt("kafka_bolt", kafkaBolt, boltNum).localOrShuffleGrouping("adaptor")
  val config = new Config()
  config.putAll(getBoltConfig(boltTopic, brokerList))
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

