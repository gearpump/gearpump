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
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{PropSpec, Matchers, BeforeAndAfterEach}

import kafka.server.{KafkaConfig => KafkaServerConfig}


class KafkaUtilSpec extends PropSpec with PropertyChecks with BeforeAndAfterEach with Matchers with KafkaServerHarness {
  val numServers = 2
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

  property("KafkaUtil should be able to create topic") {
    val zkClient = getZkClient
    forAll(Gen.alphaStr, Gen.chooseNum[Int](-10, 10), Gen.chooseNum[Int](-10, 10)) {
      (topic: String, partitions: Int, replicas: Int) =>
        if (topic.nonEmpty && partitions > 0 && replicas > 0 && replicas <= 2) {
          KafkaUtil.createTopic(zkClient, topic, partitions, replicas) shouldBe false
          KafkaUtil.createTopic(zkClient, topic, partitions, replicas) shouldBe true
        } else {
          intercept[RuntimeException] {
            KafkaUtil.createTopic(zkClient, topic, partitions, replicas)
          }
        }
    }
  }

//TODO: fix #327, This test will cause out of memory
//  property("KafkaUtil should be able to get broker info") {
//    val zkClient = getZkClient
//    val brokerList = getBrokerList
//    val servers = getServers
//    val partitionsGen = Gen.chooseNum[Int](1, 3)
//    val replicasGen = Gen.chooseNum[Int](1, numServers)
//    forAll(partitionsGen, replicasGen) {
//      (partitions: Int, replicas: Int) =>
//        val topic = TestUtils.tempTopic()
//        intercept[RuntimeException] {
//          KafkaUtil.getBroker(zkClient, topic, partitions)
//        }
//        // this createTopic will wait until leader is elected and metadata is propagated to all brokers
//        val partitionsToBrokers = TestUtils.createTopic(zkClient, topic, partitions, replicas, servers)
//        0.until(partitions).foreach { part =>
//          val broker = KafkaUtil.getBroker(zkClient, topic, part)
//          broker.toString shouldBe brokerList(partitionsToBrokers(part).get)
//        }
//    }
//  }
//
//  property("KafkaUtil should be able to get TopicAndPartitions info and group with KafkaGrouper") {
//    val zkClient = getZkClient
//    val servers = getServers
//    val grouper: KafkaGrouper = new KafkaGrouper {
//      override def group(topicAndPartitions: Array[TopicAndPartition]): Array[TopicAndPartition] = topicAndPartitions
//    }
//    forAll(Gen.chooseNum[Int](1, 5)) { (topicNum: Int) =>
//      val topics = List.fill(topicNum)(TestUtils.tempTopic())
//      topics.foreach(t => TestUtils.createTopic(zkClient, t, numPartitions = 1, replicationFactor = 1, servers))
//      KafkaUtil.getTopicAndPartitions(zkClient, grouper, topics).toSet shouldBe topics.map(t => TopicAndPartition(t, 0)).toSet
//    }
//  }
}
