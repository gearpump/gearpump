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

import java.io.InputStream
import java.util.Properties

import kafka.admin.AdminUtils
import kafka.cluster.Broker
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.gearpump.util.LogUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.Serializer
import org.slf4j.Logger

object KafkaUtil {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  def getBroker(connectZk: => ZkClient, topic: String, partition: Int): Broker = {
    val zkClient = connectZk
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

  def getTopicAndPartitions(connectZk: => ZkClient, consumerTopics: List[String]): Array[TopicAndPartition] = {
    val zkClient = connectZk
    try {
        ZkUtils.getPartitionsForTopics(zkClient, consumerTopics).flatMap {
          case (topic, partitions) => partitions.map(TopicAndPartition(topic, _))
        }.toArray
    } catch {
      case e: Exception =>
        LOG.error(e.getMessage)
        throw e
    } finally {
      zkClient.close()
    }
  }

  def topicExists(connectZk: => ZkClient, topic: String): Boolean = {
    val zkClient = connectZk
    try {
      AdminUtils.topicExists(zkClient, topic)
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
  def createTopic(connectZk: => ZkClient, topic: String, partitions: Int, replicas: Int): Boolean = {
    val zkClient = connectZk
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

  def deleteTopic(connectZk: => ZkClient, topic: String): Unit = {
    val zkClient = connectZk
    try {
      AdminUtils.deleteTopic(zkClient, topic)
    } catch {
      case e: Exception =>
        LOG.error(e.getMessage)
    } finally {
      zkClient.close()
    }
  }

  def connectZookeeper(config: ConsumerConfig): () => ZkClient = {
    val zookeeperConnect = config.zkConnect
    val sessionTimeout = config.zkSessionTimeoutMs
    val connectionTimeout = config.zkConnectionTimeoutMs
    () => new ZkClient(zookeeperConnect, sessionTimeout, connectionTimeout, ZKStringSerializer)
  }

  def loadProperties(filename: String): Properties = {
    val props = new Properties()
    var propStream: InputStream = null
    try {
      propStream = getClass.getClassLoader.getResourceAsStream(filename)
      props.load(propStream)
    } catch {
      case e: Exception =>
        LOG.error(s"$filename not found")
    } finally {
      if(propStream != null)
        propStream.close()
    }
    props
  }

  def createKafkaProducer[K, V](properties: Properties,
                                keySerializer: Serializer[K],
                                valueSerializer: Serializer[V]): KafkaProducer[K, V] = {
    if (properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) == null) {
      properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    }
    new KafkaProducer[K, V](properties, keySerializer, valueSerializer)
  }

  def buildProducerConfig(bootstrapServers: String): Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", bootstrapServers)
    properties
  }

 def buildConsumerConfig(zkConnect: String): Properties = {
   val properties = new Properties()
   properties.setProperty("zookeeper.connect", zkConnect)
   properties.setProperty("group.id", "gearpump")
   properties
  }

}
