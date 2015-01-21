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

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.common.{TopicAndPartition, TopicExistsException}
import kafka.producer.ProducerConfig
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.gearpump.streaming.kafka.lib.KafkaConsumer.Broker
import org.apache.gearpump.streaming.kafka.lib.grouper.KafkaGrouper
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

object KafkaUtil {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  def getBroker(zkClient: ZkClient, topic: String, partition: Int): Broker = {
    try {
      val leader = ZkUtils.getLeaderForPartition(zkClient, topic, partition)
        .getOrElse(throw new RuntimeException(s"leader not available for TopicAndPartition($topic, $partition)"))
      val broker = ZkUtils.getBrokerInfo(zkClient, leader)
        .getOrElse(throw new RuntimeException(s"broker info not found for leader $leader"))
      Broker(broker.host, broker.port)
    } catch {
      case e: Exception => throw e
    } finally {
      zkClient.close()
    }
  }

  def getTopicAndPartitions(zkClient: ZkClient, grouper: KafkaGrouper, consumerTopics: List[String]): Array[TopicAndPartition] = {
    try {
      val tps = grouper.group(
        ZkUtils.getPartitionsForTopics(zkClient, consumerTopics)
          .flatMap { case (topic, partitions) =>
          partitions.map(TopicAndPartition(topic, _))
        }.toArray)
      tps
    } catch {
      case e: Exception => throw e
    } finally {
      zkClient.close()
    }
  }

  /**
   *  create a new kafka topic
   *  return true if topic already exists, and false otherwise
   *
   *  Note: Do not call getBroker immediately after create topic as leader election and
   *  metadata propagation takes time
   */
  def createTopic(zkClient: ZkClient, topic: String, partitions: Int, replicas: Int): Boolean = {
    try {
      AdminUtils.createTopic(zkClient, topic, partitions, replicas)
      LOG.info(s"created topic $topic")
      false
    } catch {
      case tee: TopicExistsException =>
        LOG.info(s"topic $topic already exists")
        true
      case e: Exception => throw e
    } finally {
      zkClient.close()
    }
  }

  def buildProducerConfig(config: KafkaConfig): ProducerConfig = {
    val brokerList = config.getMetadataBrokerList
    val producerType = config.getProducerType
    val requiredAcks = config.getRequestRequiredAcks
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    props.put("producer.type", producerType)
    props.put("request.required.acks", requiredAcks)
    new ProducerConfig(props)
  }

  def connectZookeeper(config: KafkaConfig): ZkClient = {
    val zookeeperConnect = config.getZookeeperConnect
    val sessionTimeout = config.getSocketTimeoutMS
    val connectionTimeout = config.getSocketTimeoutMS
    new ZkClient(zookeeperConnect, sessionTimeout, connectionTimeout, ZKStringSerializer)
  }
}
