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
package org.apache.gearpump.streaming.kafka

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import kafka.utils.{TestZKUtils, TestUtils => TestKafkaUtils}
import kafka.server.{KafkaConfig => KafkaServerConfig}
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.kafka.util.KafkaServerHarness
import org.scalatest.{BeforeAndAfterEach, Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

import scala.collection.JavaConverters._

class KafkaDataDetectorSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfterEach with KafkaServerHarness {
  val numServers = 1
  implicit var system1: ActorSystem = null
  override val configs: List[KafkaServerConfig] = {
    for (props <- TestKafkaUtils.createBrokerConfigs(numServers, enableControlledShutdown = false))
    yield new KafkaServerConfig(props) {
      override val zkConnect = TestZKUtils.zookeeperConnect
      override val numPartitions = 1
    }
  }

  override def beforeEach(): Unit = {
    super.setUp()
    system1 = ActorSystem("KafkaDataDetector")
  }

  override def afterEach(): Unit = {
    super.tearDown()
    system1.shutdown()
  }

  property("KafkaDataDetector should return corrent partition num on each host") {
    val partitionNum = 3
    val topic = TestKafkaUtils.tempTopic()
    val brokerList = getBrokerList.mkString(",")
    createTopicUntilLeaderIsElected(topic, partitions = partitionNum, replicas = 1)

    val kafkaConfig = getKafkaConfig(topic, brokerList, zkConnect)
    val kafkaDataConnector = new KafkaDataDetector()
    kafkaDataConnector.init(kafkaConfig)
    val taskNumOnHosts = kafkaDataConnector.getTaskNumOnHosts()
    assert(taskNumOnHosts.size == 1)
    assert(taskNumOnHosts.contains("127.0.0.1"))
    assert(taskNumOnHosts.get("127.0.0.1").get == 3)
  }

  private def getKafkaConfig(consumerTopic: String, brokerList: String, zookeeperConnect: String): UserConfig = {
    UserConfig.empty.withValue[KafkaConfig](KafkaConfig.NAME, KafkaConfig(ConfigFactory.parseMap(Map(
      KafkaConfig.CONSUMER_TOPICS -> consumerTopic,
      KafkaConfig.PRODUCER_BOOTSTRAP_SERVERS -> brokerList,
      KafkaConfig.SOCKET_TIMEOUT_MS -> "1000000",
      KafkaConfig.ZOOKEEPER_CONNECT -> zookeeperConnect
    ).asJava)))
  }
}
