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

  private val DOCKER_IMAGE = "spotify/kafka"
  private val HOST = "kafka0"

  def start(): Unit = {
    Docker.run(HOST, "-d", "", DOCKER_IMAGE)
  }

  def shutDown(): Unit = {
    Docker.killAndRemove(HOST)
  }

  def getHostIpAddr: String = {
    Docker.execAndCaptureOutput(HOST, "hostname -i")
  }

  def getZookeeperPort = 2181

  def getBrokerPort = 9092

}
