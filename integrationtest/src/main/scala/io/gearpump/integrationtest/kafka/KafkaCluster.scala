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
class KafkaCluster {
  import io.gearpump.integrationtest.kafka.KafkaCluster._

  def start(): Unit = {
    Docker.run(KAFKA_HOST, s"-d -h $KAFKA_HOST", "", KAFKA_DOCKER_IMAGE)
  }

  def shutDown(): Unit = {
    Docker.killAndRemove(KAFKA_HOST)
  }

  def getHostname: String = {
    Docker.execAndCaptureOutput(KAFKA_HOST, "hostname")
  }

  def getZookeeperPort = 2181

  def getBrokerPort = 9092

}

object KafkaCluster {
  val KAFKA_DOCKER_IMAGE = "spotify/kafka"
  val KAFKA_HOST = "kafka0"
  val KAFKA_HOME = "/opt/kafka_2.11-0.8.2.1/"
}

