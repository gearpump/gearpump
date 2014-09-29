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

import com.typesafe.config.ConfigFactory
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient

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
}

class KafkaConfig {
  import KafkaConfig._

  val config = ConfigFactory.load("kafka.conf")

  def getZookeeperConnect = {
    config.getString(ZOOKEEPER_CONNECT)
  }
  def getConsumerTopic = {
    config.getString(CONSUMER_TOPIC)
  }

  def getSocketTimeoutMS = {
    config.getInt(SOCKET_TIMEOUT_MS)
  }

  def getSocketReceiveBufferSize = {
    config.getInt(SOCKET_RECEIVE_BUFFER_SIZE)
  }

  def getFetchMessageMaxBytes = {
    config.getInt(FETCH_MESSAGE_MAX_BYTES)
  }

  def getClientId = {
    config.getString(CLIENT_ID)
  }

  def getConsumerEmitBatchSize = {
    config.getInt(CONSUMER_EMIT_BATCH_SIZE)
  }

  def getZkClient = {
    val socketTimeout = getSocketTimeoutMS
    new ZkClient(getZookeeperConnect, socketTimeout, socketTimeout, ZKStringSerializer)
  }

  def getProducerEmitBatchSize = {
    config.getInt(PRODUCER_EMIT_BATCH_SIZE)
  }

  def getProducerType = {
    config.getString(PRODUCER_TYPE)
  }

  def getSerializerClass = {
    config.getString(SERIALIZER_CLASS)
  }

  def getRequestRequiredAcks = {
    config.getString(REQUEST_REQUIRED_ACKS)
  }

  def getProducerTopic = {
    config.getString(PRODUCER_TOPIC)
  }

  def getMetadataBrokerList = {
    config.getString(METADATA_BROKER_LIST)
  }
}
