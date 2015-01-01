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

import java.util.{Properties, List => JList}

import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.producer.ProducerConfig
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.gearpump.streaming.kafka.lib.grouper.KafkaGrouperFactory
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object KafkaConfig {
  // consumer config
  val ZOOKEEPER_CONNECT = "kafka.consumer.zookeeper.connect"
  val CONSUMER_TOPICS = "kafka.consumer.topics"
  val SOCKET_TIMEOUT_MS = "kafka.consumer.socket.timeout.ms"
  val SOCKET_RECEIVE_BUFFER_BYTES = "kafka.consumer.socket.receive.buffer.bytes"
  val CLIENT_ID = "kafka.consumer.client.id"
  val FETCH_MESSAGE_MAX_BYTES = "kafka.consumer.fetch.message.max.bytes"
  val CONSUMER_EMIT_BATCH_SIZE = "kafka.consumer.emit.batch.size"
  val FETCH_THRESHOLD = "kafka.consumer.fetch.threshold"
  val FETCH_SLEEP_MS = "kafka.consumer.fetch.sleep.ms"

  // producer config
  val PRODUCER_TOPIC = "kafka.producer.topic"
  val METADATA_BROKER_LIST = "kafka.producer.metadata.broker.list"
  val PRODUCER_TYPE = "kafka.producer.producer.type"
  val SERIALIZER_CLASS = "kafka.producer.serializer.class"
  val REQUEST_REQUIRED_ACKS = "kafka.producer.request.required.acks"
  val PRODUCER_EMIT_BATCH_SIZE = "kafka.producer.emit.batch.size"

  // storage config
  val STORAGE_REPLICAS = "kafka.storage.replicas"

  // grouper config
  val GROUPER_FACTORY_CLASS = "kafka.grouper.factory.class"

  def apply(): Map[String, _] = new KafkaConfig().toMap

  implicit class ConfigToKafka(config: Map[String, _]) {

    def get[T](key: String, defaultValue: Option[T] = None): T = {
      val value = config.get(key) orElse defaultValue orElse
        (throw new RuntimeException(s"${key} not found"))
      value.get.asInstanceOf[T]
    }

    def getString(key: String, defaultValue: Option[String] = None): String = {
      get[String](key, defaultValue)
    }

    def getInt(key: String, defaultValue: Option[Int] = None): Int = {
      get[Int](key, defaultValue)
    }

    def getInstance[C](key: String, defaultValue: Option[String] = None): C = {
      Class.forName(getString(key, defaultValue)).newInstance().asInstanceOf[C]
    }

    def getStringList(key: String, defaultValue: Option[JList[String]] = None): List[String] = {
      get[JList[String]](key, defaultValue).asScala.toList
    }

    def getConsumer(topicAndPartitions: Array[TopicAndPartition],
                    clientId: String = getClientId,
                    socketTimeout: Int = getSocketTimeoutMS,
                    receiveBufferSize: Int = getSocketReceiveBufferBytes,
                    fetchSize: Int = getFetchMessageMaxBytes,
                    zkClient: ZkClient = getZkClient(),
                    fetchThreshold: Int = getFetchThreshold,
                    fetchSleepMS: Int = getFetchSleepMS): KafkaConsumer = {
      KafkaConsumer(topicAndPartitions, clientId, socketTimeout,
        receiveBufferSize, fetchSize, zkClient, fetchThreshold, fetchSleepMS)
    }

    def getZookeeperConnect: String = {
      getString(ZOOKEEPER_CONNECT)
    }

    def getConsumerTopics: List[String] = {
      getStringList(CONSUMER_TOPICS)
    }

    def getSocketTimeoutMS: Int = {
      getInt(SOCKET_TIMEOUT_MS)
    }

    def getSocketReceiveBufferBytes: Int = {
      getInt(SOCKET_RECEIVE_BUFFER_BYTES)
    }

    def getFetchMessageMaxBytes: Int = {
      getInt(FETCH_MESSAGE_MAX_BYTES)
    }

    def getClientId: String = {
      getString(CLIENT_ID)
    }

    def getConsumerEmitBatchSize: Int = {
      getInt(CONSUMER_EMIT_BATCH_SIZE)
    }

    def getFetchSleepMS: Int = {
      getInt(FETCH_SLEEP_MS)
    }

    def getFetchThreshold: Int = {
      getInt(FETCH_THRESHOLD)
    }

    def getZkClient(zookeeperConnect: String = getZookeeperConnect,
                    sessionTimeout: Int = getSocketTimeoutMS,
                    connectionTimeout: Int = getSocketTimeoutMS,
                    zkSerializer: ZkSerializer = ZKStringSerializer): ZkClient = {
      new ZkClient(zookeeperConnect, sessionTimeout, connectionTimeout, ZKStringSerializer)
    }

    def getProducer[K, V](producerConfig: ProducerConfig = getProducerConfig(),
                          emitBatchSize: Int = getProducerEmitBatchSize): KafkaProducer[K, V] = {
      KafkaProducer[K, V](producerConfig, emitBatchSize)
    }

    def getProducerConfig(brokerList: String = getMetadataBrokerList,
                          serializerClass: String = getSerializerClass,
                          producerType: String = getProducerType,
                          requiredAcks: String = getRequestRequiredAcks): ProducerConfig = {
      val props = new Properties()
      props.put("metadata.broker.list", brokerList)
      props.put("serializer.class", serializerClass)
      props.put("producer.type", producerType)
      props.put("request.required.acks", requiredAcks)
      new ProducerConfig(props)
    }

    def getProducerTopic: String = {
      getString(PRODUCER_TOPIC)
    }

    def getProducerEmitBatchSize: Int = {
      getInt(PRODUCER_EMIT_BATCH_SIZE)
    }

    def getProducerType: String = {
      getString(PRODUCER_TYPE)
    }

    def getSerializerClass: String = {
      getString(SERIALIZER_CLASS)
    }

    def getRequestRequiredAcks: String = {
      getString(REQUEST_REQUIRED_ACKS)
    }

    def getMetadataBrokerList: String = {
      getString(METADATA_BROKER_LIST)
    }

    def getGrouperFactory: KafkaGrouperFactory = {
      getInstance[KafkaGrouperFactory](GROUPER_FACTORY_CLASS)
    }

    def getStorageReplicas: Int = {
      getInt(STORAGE_REPLICAS)
    }
  }

  private val LOG: Logger = LogUtil.getLogger(getClass)
}

class KafkaConfig {
  import org.apache.gearpump.streaming.kafka.lib.KafkaConfig._

  LOG.info("Loading Kafka configurations...")
  val config = ConfigFactory.load("kafka.conf")

  def toMap: Map[String, _] = {
    config.entrySet.map(entry => (entry.getKey, entry.getValue.unwrapped)).toMap
  }
}
