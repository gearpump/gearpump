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

package io.gearpump.streaming.kafka.util

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.common.KafkaException
import kafka.server.{KafkaConfig => KafkaServerConfig, KafkaServer}
import kafka.utils.{TestUtils, Utils}

trait KafkaServerHarness extends ZookeeperHarness {
  val configs: List[KafkaServerConfig]
  private var servers: List[KafkaServer] = null
  private var brokerList: String = null

  def getServers: List[KafkaServer] = servers
  def getBrokerList: Array[String] = brokerList.split(",")

  override def setUp() {
    super.setUp
    if (configs.size <= 0) {
      throw new KafkaException("Must supply at least one server config.")
    }
    brokerList = TestUtils.getBrokerListStrFromConfigs(configs)
    servers = configs.map(TestUtils.createServer(_))
  }

  override def tearDown() {
    servers.map(server => server.shutdown())
    servers.map(server => server.config.logDirs.map(Utils.rm(_)))
    super.tearDown
  }

  def createTopicUntilLeaderIsElected(
      topic: String, partitions: Int, replicas: Int, timeout: Long = 10000)
    : Map[Int, Option[Int]] = {
    val zkClient = connectZk()
    try {
      // create topic
      AdminUtils.createTopic(zkClient, topic, partitions, replicas, new Properties)
      // wait until the update metadata request for new topic reaches all servers
      (0 until partitions).map { case i =>
        TestUtils.waitUntilMetadataIsPropagated(servers, topic, i, timeout)
        i -> TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, i, timeout)
      }.toMap
    } catch {
      case e: Exception => throw e
    } finally {
      zkClient.close()
    }
  }
}
