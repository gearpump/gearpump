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
package org.apache.gearpump.streaming.kafka

import java.net.InetAddress

import akka.actor.ActorSystem
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.appmaster.DataDetector
import org.apache.gearpump.streaming.kafka.lib.{KafkaUtil, KafkaConfig}

class KafkaDataDetector extends DataDetector {
  private var taskNumOnHosts = Map.empty[String, Int]

  override def init(config: UserConfig)(implicit actorSystem: ActorSystem): Unit = {
    val kafkaConfig = config.getValue[KafkaConfig](KafkaConfig.NAME).get
    val zkClient = KafkaUtil.connectZookeeper(kafkaConfig)
    val topic = kafkaConfig.getConsumerTopics
    val topicAndPartitions = ZkUtils.getPartitionsForTopics(zkClient, topic)
    topicAndPartitions.foreach{ kv =>
      val (topic, partitions) = kv
      partitions.foreach{ partitionId =>
        val ip = getHostIPForPartition(zkClient, topic, partitionId)
        val taskNums = taskNumOnHosts.getOrElse(ip, 0) + 1
        taskNumOnHosts += ip -> taskNums
      }
    }
  }

  override def getTaskNumOnHosts(): Map[String, Int] = taskNumOnHosts

  private def getHostIPForPartition(zkClient: ZkClient, topic: String, partition: Int): String = {
    val leader = ZkUtils.getLeaderForPartition(zkClient, topic, partition)
      .getOrElse(throw new RuntimeException(s"leader not available for TopicAndPartition($topic, $partition)"))
    val broker = ZkUtils.getBrokerInfo(zkClient, leader)
      .getOrElse(throw new RuntimeException(s"broker info not found for leader $leader"))
    InetAddress.getByName(broker.host).getHostAddress
  }
}
