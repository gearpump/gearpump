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
package org.apache.gearpump.integrationtest.kafka

import org.apache.log4j.Logger

import org.apache.gearpump.integrationtest.minicluster.MiniCluster
import org.apache.gearpump.integrationtest.{Docker, Util}

object KafkaCluster {

  /** Starts a Kafka cluster */
  def withKafkaCluster(cluster: MiniCluster)(testCode: KafkaCluster => Unit): Unit = {
    val kafkaCluster = new KafkaCluster(cluster.getNetworkGateway, "kafka")
    try {
      kafkaCluster.start()
      testCode(kafkaCluster)
    } finally {
      kafkaCluster.shutDown()
    }
  }

  def withDataProducer(topic: String, brokerList: String)
    (testCode: NumericalDataProducer => Unit): Unit = {
    val producer = new NumericalDataProducer(topic, brokerList)
    try {
      producer.start()
      testCode(producer)
    } finally {
      producer.stop()
    }
  }
}

/**
 * This class maintains a single node Kafka cluster with integrated Zookeeper.
 */
class KafkaCluster(val advertisedHost: String, zkChroot: String = "") {

  private val LOG = Logger.getLogger(getClass)
  private val KAFKA_DOCKER_IMAGE = "spotify/kafka"
  private val KAFKA_HOST = "kafka0"
  private val KAFKA_HOME = "/opt/kafka_2.11-0.8.2.1/"
  private val ZOOKEEPER_PORT = 2181
  private val BROKER_PORT = 9092
  val advertisedPort = BROKER_PORT

  def start(): Unit = {
    Docker.createAndStartContainer(KAFKA_HOST, KAFKA_DOCKER_IMAGE, "",
      environ = Map(
        "ADVERTISED_HOST" -> advertisedHost,
        "ADVERTISED_PORT" -> BROKER_PORT.toString,
        "ZK_CHROOT" -> zkChroot),
      tunnelPorts = Set(ZOOKEEPER_PORT, BROKER_PORT)
    )
    Util.retryUntil(() => isAlive, "kafka cluster is alive")
    LOG.debug("kafka cluster is started.")
  }

  def isAlive: Boolean = {
    !listTopics().contains("Connection refused")
  }

  def shutDown(): Unit = {
    Docker.killAndRemoveContainer(KAFKA_HOST)
  }

  private lazy val hostIPAddr = Docker.getContainerIPAddr(KAFKA_HOST)

  def listTopics(): String = {
    kafkaListTopics(KAFKA_HOST, KAFKA_HOME, getZookeeperConnectString)
  }

  def getZookeeperConnectString: String = {
    s"$hostIPAddr:$ZOOKEEPER_PORT/$zkChroot"
  }

  def getBrokerListConnectString: String = {
    s"$hostIPAddr:$BROKER_PORT"
  }

  def createTopic(topic: String, partitions: Int = 1): Unit = {
    LOG.debug(s"|=> Create kafka topic $topic with $partitions partitions")

    Docker.executeSilently(KAFKA_HOST,
      s"$KAFKA_HOME/bin/kafka-topics.sh" +
        s" --zookeeper $getZookeeperConnectString" +
        s" --create --topic $topic --partitions $partitions --replication-factor 1")
  }

  def produceDataToKafka(topic: String, messageNum: Int): Unit = {
    Docker.executeSilently(KAFKA_HOST,
      s"$KAFKA_HOME/bin/kafka-topics.sh" +
        s" --zookeeper $getZookeeperConnectString" +
        s" --create --topic $topic --partitions 1 --replication-factor 1")

    Docker.executeSilently(KAFKA_HOST,
      s"$KAFKA_HOME/bin/kafka-producer-perf-test.sh" +
        s" --broker-list $getBrokerListConnectString" +
        s" --topic $topic --messages $messageNum")
  }

  def getLatestOffset(topic: String): Int = {
    kafkaFetchLatestOffset(KAFKA_HOST, topic, KAFKA_HOME, getBrokerListConnectString)
  }

  private def kafkaListTopics(
      container: String, kafkaHome: String, zookeeperConnectionString: String): String = {

    LOG.debug(s"|=> Kafka list topics...")
    Docker.execute(container,
      s"$kafkaHome/bin/kafka-topics.sh" +
        s" --zookeeper $zookeeperConnectionString -list")
  }

  private def kafkaFetchLatestOffset(
      container: String, topic: String, kafkaHome: String, brokersList: String): Int = {
    LOG.debug(s"|=> Get latest offset of topic $topic...")
    val output = Docker.execute(container,
      s"$kafkaHome/bin/kafka-run-class.sh kafka.tools.GetOffsetShell" +
        s" --broker-list $brokersList " +
        s" --topic $topic --time -1")
    output.split(":")(2).toInt
  }
}
