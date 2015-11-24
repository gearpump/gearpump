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
package io.gearpump.integrationtest.kafka

import io.gearpump.integrationtest.Docker

/**
 * This class maintains a single node Kafka cluster with integrated Zookeeper.
 */
class KafkaCluster(val advertisedHost: String) {

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
        "ADVERTISED_PORT" -> BROKER_PORT.toString),
      tunnelPorts = Set(ZOOKEEPER_PORT, BROKER_PORT)
    )
  }

  def shutDown(): Unit = {
    Docker.killAndRemoveContainer(KAFKA_HOST)
  }

  private lazy val hostIPAddr = Docker.getContainerIPAddr(KAFKA_HOST)

  def getZookeeperConnectString: String = {
    s"$hostIPAddr:$ZOOKEEPER_PORT"
  }

  def getBrokerListConnectString: String = {
    s"$hostIPAddr:$BROKER_PORT"
  }

  def createTopic(sourceTopic: String, partitions: Int = 1): Unit = {
    Docker.exec(KAFKA_HOST,
      s"$KAFKA_HOME/bin/kafka-topics.sh" +
        s" --zookeeper $getZookeeperConnectString" +
        s" --create --topic $sourceTopic --partitions $partitions --replication-factor 1")
  }

  def produceDataToKafka(sourceTopic: String, messageNum: Int): Unit = {
    Docker.exec(KAFKA_HOST,
      s"$KAFKA_HOME/bin/kafka-topics.sh" +
        s" --zookeeper $getZookeeperConnectString" +
        s" --create --topic $sourceTopic --partitions 1 --replication-factor 1")

    Docker.exec(KAFKA_HOST,
      s"$KAFKA_HOME/bin/kafka-producer-perf-test.sh" +
        s" --broker-list $getBrokerListConnectString" +
        s" --topic $sourceTopic --messages $messageNum")
  }

  def getLatestOffset(sinkTopic: String): Int = {
    val output = Docker.execAndCaptureOutput(KAFKA_HOST,
      s"$KAFKA_HOME/bin/kafka-run-class.sh kafka.tools.GetOffsetShell" +
        s" --broker-list $getBrokerListConnectString " +
        s" --topic $sinkTopic --time -1")
    output.split(":")(2).toInt
  }

}
