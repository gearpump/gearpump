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

package gearpump.streaming.kafka.lib

import java.util.Properties

import gearpump.streaming.kafka.lib.grouper.KafkaGrouper
import kafka.admin.AdminUtils
import kafka.cluster.Broker
import kafka.common.TopicAndPartition
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import gearpump.util.LogUtil
import org.slf4j.Logger

object KafkaUtil {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  def getBroker(zkClient: ZkClient, topic: String, partition: Int): Broker = {
    try {
      val leader = ZkUtils.getLeaderForPartition(zkClient, topic, partition)
        .getOrElse(throw new RuntimeException(s"leader not available for TopicAndPartition($topic, $partition)"))
      ZkUtils.getBrokerInfo(zkClient, leader)
        .getOrElse(throw new RuntimeException(s"broker info not found for leader $leader"))
    } catch {
      case e: Exception =>
        LOG.error(e.getMessage)
        throw e
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
      case e: Exception =>
        LOG.error(e.getMessage)
        throw e
    } finally {
      zkClient.close()
    }
  }

  /**
   *  create a new kafka topic
   *  return true if topic already exists, and false otherwise
   */
  def createTopic(zkClient: ZkClient, topic: String, partitions: Int, replicas: Int): Boolean = {
    try {
      if (AdminUtils.topicExists(zkClient, topic)) {
        LOG.info(s"topic $topic exists")
        true
      } else {
        AdminUtils.createTopic(zkClient, topic, partitions, replicas)
        LOG.info(s"created topic $topic")
        false
      }
    } catch {
      case e: Exception =>
        LOG.error(e.getMessage)
        throw e
    } finally {
      zkClient.close()
    }
  }


  def buildProducerConfig(config: KafkaConfig): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", config.getProducerBootstrapServers)
    props.put("acks", config.getProducerAcks)
    // scala Int/Long extends AnyVal while this requires AnyRef (java Object)
    props.put("buffer.memory", config.getProducerBufferMemory.asInstanceOf[java.lang.Long])
    props.put("compression.type", config.getProducerCompressionType)
    props.put("batch.size", config.getProducerBatchSize.asInstanceOf[java.lang.Integer])
    props.put("retries", config.getProducerRetries.asInstanceOf[java.lang.Integer])
    props
  }

  def connectZookeeper(config: KafkaConfig): ZkClient = {
    val zookeeperConnect = config.getZookeeperConnect
    val sessionTimeout = config.getSocketTimeoutMS
    val connectionTimeout = config.getSocketTimeoutMS
    new ZkClient(zookeeperConnect, sessionTimeout, connectionTimeout, ZKStringSerializer)
  }
}
