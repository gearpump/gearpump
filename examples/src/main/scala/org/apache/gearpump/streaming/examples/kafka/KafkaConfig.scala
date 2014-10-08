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

package org.apache.gearpump.streaming.examples.kafka

import java.util.Properties

import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.producer.ProducerConfig
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object KafkaConfig {
  // consumer config
  val ZOOKEEPER_CONNECT = "kafka.consumer.zookeeper.connect"
  val CONSUMER_TOPIC = "kafka.consumer.topic"
  val SOCKET_TIMEOUT_MS = "kafka.consumer.socket.timeout.ms"
  val SOCKET_RECEIVE_BUFFER_SIZE = "kafka.consumer.socket.receive.buffer.size"
  val CLIENT_ID = "kafka.consumer.client.id"
  val FETCH_MESSAGE_MAX_BYTES = "kafka.consumer.fetch.message.max.bytes"
  val CONSUMER_EMIT_BATCH_SIZE = "kafka.consumer.emit.batch.size"

  // producer config
  val PRODUCER_TOPIC = "kafka.producer.topic"
  val METADATA_BROKER_LIST = "kafka.producer.metadata.broker.list"
  val PRODUCER_TYPE = "kafka.producer.producer.type"
  val SERIALIZER_CLASS = "kafka.producer.serializer.class"
  val REQUEST_REQUIRED_ACKS = "kafka.producer.request.required.acks"
  val PRODUCER_EMIT_BATCH_SIZE = "kafka.producer.emit.batch.size"

  // grouper config
  val GROUPER_CLASS = "kafka.grouper.class"

  def apply(): Map[String, _] = new KafkaConfig().toMap

  implicit class ConfigToKafka(config: Map[String, _]) {

    private def getString(key: String): String = {
      config.get(key).get.asInstanceOf[String]
    }

    private def getInt(key: String): Int = {
      config.get(key).get.asInstanceOf[Int]
    }

    private def getStringList(key: String): List[String] = {
      config.get(key).get.asInstanceOf[java.util.List[String]].asScala.toList
    }

    def getConsumer(topicAndPartitions: Array[TopicAndPartition],
                    clientId: String = getClientId,
                    socketTimeout: Int = getSocketTimeoutMS,
                    receiveBufferSize: Int = getSocketReceiveBufferSize,
                    fetchSize: Int = getFetchMessageMaxBytes,
                    zkClient: ZkClient = getZkClient()): KafkaConsumer = {
      new KafkaConsumer(topicAndPartitions, clientId, socketTimeout,
        receiveBufferSize, fetchSize, zkClient)
    }

    def getZookeeperConnect = {
      getString(ZOOKEEPER_CONNECT)
    }

    def getConsumerTopics = {
      getStringList(CONSUMER_TOPIC)
    }

    def getSocketTimeoutMS = {
      getInt(SOCKET_TIMEOUT_MS)
    }

    def getSocketReceiveBufferSize = {
      getInt(SOCKET_RECEIVE_BUFFER_SIZE)
    }

    def getFetchMessageMaxBytes = {
      getInt(FETCH_MESSAGE_MAX_BYTES)
    }

    def getClientId = {
      getString(CLIENT_ID)
    }

    def getConsumerEmitBatchSize = {
      getInt(CONSUMER_EMIT_BATCH_SIZE)
    }

    def getZkClient(zookeeperConnect: String = getZookeeperConnect,
                    sessionTimeout: Int = getSocketTimeoutMS,
                    connectionTimeout: Int = getSocketTimeoutMS,
                    zkSerializer: ZkSerializer = ZKStringSerializer) = {
      new ZkClient(zookeeperConnect, sessionTimeout, connectionTimeout, ZKStringSerializer)
    }



    def getProducer[K, V](producerConfig: ProducerConfig = getProducerConfig(),
                          emitBatchSize: Int = getProducerEmitBatchSize): KafkaProducer[K, V] = {
      new KafkaProducer[K, V](producerConfig, emitBatchSize)
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

    def getProducerTopic = {
      getString(PRODUCER_TOPIC)
    }

    def getProducerEmitBatchSize = {
      getInt(PRODUCER_EMIT_BATCH_SIZE)
    }

    def getProducerType = {
      getString(PRODUCER_TYPE)
    }

    def getSerializerClass = {
      getString(SERIALIZER_CLASS)
    }

    def getRequestRequiredAcks = {
      getString(REQUEST_REQUIRED_ACKS)
    }

    def getMetadataBrokerList = {
      getString(METADATA_BROKER_LIST)
    }

    def getGrouper(): Grouper = {
      Class.forName(getString(GROUPER_CLASS)).newInstance().asInstanceOf[Grouper]
    }
  }

  private val LOG: Logger = LoggerFactory.getLogger(classOf[KafkaConfig])
}

class KafkaConfig {

  import org.apache.gearpump.streaming.examples.kafka.KafkaConfig._

  LOG.info("Loading Kafka configurations...")
  val config = ConfigFactory.load("kafka.conf")

  def toMap: Map[String, _] = {
    config.entrySet.map(entry => (entry.getKey, entry.getValue.unwrapped)).toMap
  }
}
