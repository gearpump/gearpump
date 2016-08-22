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
package org.apache.gearpump.streaming.kafka.lib.util

import java.util.Properties

import kafka.common.TopicAndPartition
import kafka.server.{KafkaConfig => KafkaServerConfig}
import kafka.utils.{TestZKUtils, TestUtils}
import org.apache.gearpump.streaming.kafka.lib.source.consumer.KafkaConsumer
import org.apache.gearpump.streaming.kafka.util.{KafkaConfig, KafkaServerHarness}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Ignore, Matchers, BeforeAndAfterEach, PropSpec}

// Ignore since KafkaClientSpec randomly fails on Travis
@Ignore
class KafkaClientSpec extends PropSpec with PropertyChecks
  with BeforeAndAfterEach with Matchers with KafkaServerHarness {

  val numServers = 1
  override val configs: List[KafkaServerConfig] =
    for (props <- TestUtils.createBrokerConfigs(numServers, enableControlledShutdown = false))
      yield new KafkaServerConfig(props) {
        override val zkConnect = TestZKUtils.zookeeperConnect
        override val numPartitions = 4
      }

  override def beforeEach(): Unit = {
    super.setUp()
  }

  override def afterEach(): Unit = {
    super.tearDown()
  }


  property("KafkaClient should be able to create topic") {
    val args = {
      Table(
        ("topic", "partitions", "replicas"),
        (TestUtils.tempTopic(), 1, numServers),
        (TestUtils.tempTopic(), 2, numServers + 1),
        ("", 1, numServers),
        (TestUtils.tempTopic(), 0, numServers),
        (TestUtils.tempTopic(), 1, 0)
      )
    }
    forAll(args) {
      (topic: String, partitions: Int, replicas: Int) =>
        val props = new Properties
        props.put(KafkaConfig.ZOOKEEPER_CONNECT_CONFIG, zkConnect)
        props.put(KafkaConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerList)
        val kafkaClient = new KafkaClient(new KafkaConfig(props), getZkClient)
        if (topic.nonEmpty && partitions > 0 && replicas > 0 && replicas <= numServers) {

          kafkaClient.createTopic(topic, partitions, replicas) shouldBe false
          kafkaClient.createTopic(topic, partitions, replicas) shouldBe true
        } else {
          intercept[RuntimeException] {
            kafkaClient.createTopic(topic, partitions, replicas)
          }
        }
    }
  }

  property("KafkaClient should be able to get broker info") {
    val brokerList = getBrokerList.split(",", -1)
    val partitions = 2
    val replicas = numServers
    val topic = TestUtils.tempTopic()
    val props = new Properties
    props.put(KafkaConfig.ZOOKEEPER_CONNECT_CONFIG, zkConnect)
    props.put(KafkaConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerList)
    val kafkaClient = new KafkaClient(new KafkaConfig(props), getZkClient)
    intercept[RuntimeException] {
      kafkaClient.getBroker(topic, partitions)
    }
    val partitionsToBrokers = createTopicUntilLeaderIsElected(topic, partitions, replicas)
    0.until(partitions).foreach { part =>
      val hostPorts = brokerList(partitionsToBrokers(part).get).split(":")
      val broker = kafkaClient.getBroker(topic, part)

      broker.host shouldBe hostPorts(0)
      broker.port.toString shouldBe hostPorts(1)
    }
  }

  property("KafkaClient should be able to get TopicAndPartitions info") {
    val props = new Properties
    props.put(KafkaConfig.ZOOKEEPER_CONNECT_CONFIG, zkConnect)
    props.put(KafkaConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerList)
    val kafkaClient = new KafkaClient(new KafkaConfig(props), getZkClient)
    val topicNum = 3
    val topics = List.fill(topicNum)(TestUtils.tempTopic())
    topics.foreach(t => createTopicUntilLeaderIsElected(t, partitions = 1, replicas = 1))
    val actual = kafkaClient.getTopicAndPartitions(topics).toSet
    val expected = topics.map(t => TopicAndPartition(t, 0)).toSet
    actual shouldBe expected
  }

  property("KafkaClient should be able to create consumer") {
    val props = new Properties
    props.put(KafkaConfig.ZOOKEEPER_CONNECT_CONFIG, zkConnect)
    props.put(KafkaConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerList)
    val kafkaClient = new KafkaClient(new KafkaConfig(props), getZkClient)
    val partitions = 2
    val topic = TestUtils.tempTopic()
    createTopicUntilLeaderIsElected(topic, partitions, replicas = 1)
    0.until(partitions).foreach { par =>
      kafkaClient.createConsumer(topic, par,
        startOffsetTime = -2) shouldBe a [KafkaConsumer]
    }

  }

  property("KafkaClient should be able to create producer") {
    val props = new Properties
    props.put(KafkaConfig.ZOOKEEPER_CONNECT_CONFIG, zkConnect)
    props.put(KafkaConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerList)
    val kafkaClient = new KafkaClient(new KafkaConfig(props), getZkClient)
    kafkaClient.createProducer(new ByteArraySerializer,
      new ByteArraySerializer) shouldBe a [KafkaProducer[_, _]]
  }
}
