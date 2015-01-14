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

import java.util.{List => JList}

import com.typesafe.config.Config
import org.apache.gearpump.streaming.kafka.lib.grouper.KafkaGrouperFactory
import org.apache.gearpump.streaming.transaction.api.{TimeStampFilter, MessageDecoder}
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.collection.JavaConverters._
import scala.util.Try

object KafkaConfig {

  val NAME = "kafkaConfig"

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
  val REQUEST_REQUIRED_ACKS = "kafka.producer.request.required.acks"
  val PRODUCER_EMIT_BATCH_SIZE = "kafka.producer.emit.batch.size"

  // storage config
  val STORAGE_REPLICAS = "kafka.storage.replicas"

  // grouper config
  val GROUPER_FACTORY_CLASS = "kafka.grouper.factory.class"

  // task config
  val MESSAGE_DECODER_CLASS = "kafka.task.message.decoder.class"
  val TIMESTAMP_FILTER_CLASS = "kafka.task.timestamp.filter.class"

  def apply(config: Config): KafkaConfig = new KafkaConfig(config)

  private val LOG: Logger = LogUtil.getLogger(getClass)
}

class KafkaConfig(config: Config) extends Serializable  {
  import org.apache.gearpump.streaming.kafka.lib.KafkaConfig._

  private def get[T](key: String, optValue: Option[T], defaultValue: Option[T] = None): T = {
    val value = optValue orElse defaultValue orElse (throw new RuntimeException(s"$key not found"))
    value.get
  }

  private def getString(key: String, defaultValue: Option[String] = None): String = {
    get[String](key, Try(config.getString(key)).toOption, defaultValue)
  }

  private def getInt(key: String, defaultValue: Option[Int] = None): Int = {
    get[Int](key, Try(config.getInt(key)).toOption, defaultValue)
  }


  private def getInstance[C](key: String, defaultValue: Option[String] = None): C = {
    Class.forName(getString(key, defaultValue)).newInstance().asInstanceOf[C]
  }

  def getZookeeperConnect: String = {
    getString(ZOOKEEPER_CONNECT)
  }

  def getConsumerTopics: List[String] = {
    getString(CONSUMER_TOPICS).split(",").toList
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

  def getProducerTopic: String = {
    getString(PRODUCER_TOPIC)
  }

  def getProducerEmitBatchSize: Int = {
    getInt(PRODUCER_EMIT_BATCH_SIZE)
  }

  def getProducerType: String = {
    getString(PRODUCER_TYPE)
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

  def getMessageDecoder: MessageDecoder = {
    getInstance[MessageDecoder](MESSAGE_DECODER_CLASS)
  }

  def getTimeStampFilter: TimeStampFilter = {
    getInstance[TimeStampFilter](TIMESTAMP_FILTER_CLASS)
  }

}

