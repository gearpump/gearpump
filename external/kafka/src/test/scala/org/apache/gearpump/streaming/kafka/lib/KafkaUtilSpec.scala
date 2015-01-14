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

package org.apache.gearpump.streaming.kafka.lib

import kafka.common.TopicAndPartition
import kafka.utils.{TestZKUtils, TestUtils}
import org.apache.gearpump.streaming.kafka.lib.grouper.KafkaGrouper
import org.apache.gearpump.streaming.kafka.util.KafkaServerHarness
import org.scalatest.prop.PropertyChecks
import org.scalatest.{PropSpec, Matchers, BeforeAndAfterEach}

import kafka.server.{KafkaConfig => KafkaServerConfig}


class KafkaUtilSpec extends PropSpec with PropertyChecks with BeforeAndAfterEach with Matchers with KafkaServerHarness {
  val numServers = 1
  override val configs: List[KafkaServerConfig] =
    for(props <- TestUtils.createBrokerConfigs(numServers, enableControlledShutdown = false))
    yield new KafkaServerConfig(props) {
      override val zkConnect = TestZKUtils.zookeeperConnect
      override val numPartitions = 4
    }

  override def beforeEach: Unit = {
    super.setUp()
  }

  override def afterEach: Unit = {
    super.tearDown()
  }

  import org.apache.gearpump.streaming.kafka.lib.KafkaUtil._

  property("KafkaUtil should be able to create topic") {
    val args = {
      Table(
        ("topic", "partitions", "replicas"),
        (TestUtils.tempTopic, 1, numServers),
        (TestUtils.tempTopic, 2, numServers + 1),
        ("", 1, numServers),
        (TestUtils.tempTopic, 0, numServers),
        (TestUtils.tempTopic, 1, 0)
      )
    }
    forAll(args) {
      (topic: String, partitions: Int, replicas: Int) =>
        if (topic.nonEmpty && partitions > 0 && replicas > 0 && replicas <= numServers) {
          createTopic(newZkClient, topic, partitions, replicas) shouldBe false
          createTopic(newZkClient, topic, partitions, replicas) shouldBe true
        } else {
          intercept[RuntimeException] {
            createTopic(newZkClient, topic, partitions, replicas)
          }
        }
    }
  }

  property("KafkaUtil should be able to get broker info") {
    val brokerList = getBrokerList
    val partitions = 2
    val replicas = numServers
    val topic = TestUtils.tempTopic()
    intercept[RuntimeException] {
      getBroker(newZkClient, topic, partitions)
    }
    val partitionsToBrokers = createTopicUntilLeaderIsElected(topic, partitions, replicas)
    0.until(partitions).foreach { part =>
      val broker = getBroker(newZkClient, topic, part)
      broker.toString shouldBe brokerList(partitionsToBrokers(part).get)
    }
  }

  property("KafkaUtil should be able to get TopicAndPartitions info and group with KafkaGrouper") {
    val grouper: KafkaGrouper = new KafkaGrouper {
      override def group(topicAndPartitions: Array[TopicAndPartition]): Array[TopicAndPartition] = topicAndPartitions
    }
    val topicNum = 3
    val topics = List.fill(topicNum)(TestUtils.tempTopic())
    topics.foreach(t => createTopicUntilLeaderIsElected(t, partitions = 1, replicas = 1))
    KafkaUtil.getTopicAndPartitions(newZkClient, grouper, topics).toSet shouldBe topics.map(t => TopicAndPartition(t, 0)).toSet
  }

}
