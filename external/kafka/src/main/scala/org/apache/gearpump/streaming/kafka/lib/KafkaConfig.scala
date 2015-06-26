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

import com.typesafe.config.{ConfigValue, ConfigFactory, ConfigValueFactory, Config}
import org.apache.gearpump.streaming.kafka.lib.grouper.KafkaGrouperFactory
import org.apache.gearpump.streaming.transaction.api.{TimeStampFilter, MessageDecoder}
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

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
  val PRODUCER_BOOTSTRAP_SERVERS = "kafka.producer.bootstrap.servers"
  val PRODUCER_ACKS = "kafka.producer.acks"
  val PRODUCER_BUFFER_MEMORY = "kafka.producer.buffer.memory"
  val PRODUCER_COMPRESSION_TYPE = "kafka.producer.compression.type"
  val PRODUCER_BATCH_SIZE = "kafka.producer.batch.size"
  val PRODUCER_RETRIES = "kafka.producer.retries"

  // storage config
  val STORAGE_REPLICAS = "kafka.storage.replicas"

  // grouper config
  val GROUPER_FACTORY_CLASS = "kafka.grouper.factory.class"

  // task config
  val MESSAGE_DECODER_CLASS = "kafka.task.message.decoder.class"
  val TIMESTAMP_FILTER_CLASS = "kafka.task.timestamp.filter.class"

  def apply(config: Config): KafkaConfig = new KafkaConfig(config)

  def apply(resource: String): KafkaConfig = KafkaConfig(ConfigFactory.parseResources(resource))

  def apply(): KafkaConfig = KafkaConfig(ConfigFactory.empty())

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

  private def getLong(key: String, defaultValue: Option[Long] = None): Long = {
    get[Long](key, Try(config.getLong(key)).toOption, defaultValue)
  }

  private def getInstance[C](key: String, defaultValue: Option[String] = None): C = {
    Class.forName(getString(key, defaultValue)).newInstance().asInstanceOf[C]
  }

  private def configValue(anyRef: AnyRef): ConfigValue = {
    ConfigValueFactory.fromAnyRef(anyRef)
  }

  /**
   * set kafka consumer config `zookeeper.connect`
   * @param zkConnect the value of zookeeper connect string
   * @return new KafkaConfig based on this but with [[ZOOKEEPER_CONNECT]] set to given value
   */
  def withZookeeperConnect(zkConnect: String): KafkaConfig = {
    KafkaConfig(config.withValue(ZOOKEEPER_CONNECT, configValue(zkConnect)))
  }

  /**
   * @return kafka consumer config `zookeeper.connect`
   */
  def getZookeeperConnect: String = {
    getString(ZOOKEEPER_CONNECT)
  }

  /**
   * set kafka consumer topics
   * @param topics comma-separated string
   * @return new KafkaConfig based on this but with [[CONSUMER_TOPICS]] set to given value
   */
  def withConsumerTopics(topics: String): KafkaConfig = {
    KafkaConfig(config.withValue(CONSUMER_TOPICS, configValue(topics)))
  }

  /**
   * @return a list of kafka consumer topics
   */
  def getConsumerTopics: List[String] = {
    getString(CONSUMER_TOPICS).split(",").toList
  }

  /**
   * set kafka consumer config `socket.timeout.ms`
   * @param timeout timeout in milliseconds
   * @return new KafkaConfig based on this but with [[SOCKET_TIMEOUT_MS]] set to given value
   */
  def withSocketTimeoutMS(timeout: Int): KafkaConfig = {
    KafkaConfig(config.withValue(SOCKET_TIMEOUT_MS, configValue(timeout)))
  }

  /**
   * @return kafka consumer config `socket.timeout.ms`
   */
  def getSocketTimeoutMS: Int = {
    getInt(SOCKET_TIMEOUT_MS)
  }

  /**
   * set kafka consumer config `socket.receive.buffer.bytes`
   * @param bytes buffer size in bytes
   * @return new KafkaConfig based on this but with [[SOCKET_RECEIVE_BUFFER_BYTES]] set to given value
   */
  def withSocketReceiveBufferBytes(bytes: Int): KafkaConfig = {
    KafkaConfig(config.withValue(SOCKET_RECEIVE_BUFFER_BYTES, configValue(bytes)))
  }

  /**
   * @return kafka consumer config `socket.receive.buffer.bytes`
   */
  def getSocketReceiveBufferBytes: Int = {
    getInt(SOCKET_RECEIVE_BUFFER_BYTES)
  }

  /**
   * set kafka consumer config `fetch.message.max.bytes`
   * @param bytes The number of byes of messages
   * @return new KafkaConfig based on this but with [[FETCH_MESSAGE_MAX_BYTES]] set to given value
   */
  def withFetchMessageMaxBytes(bytes: Int): KafkaConfig = {
    KafkaConfig(config.withValue(FETCH_MESSAGE_MAX_BYTES, configValue(bytes)))
  }

  /**
   * @return kafka consumer config `fetch.message.max.bytes`
   */
  def getFetchMessageMaxBytes: Int = {
    getInt(FETCH_MESSAGE_MAX_BYTES)
  }

  /**
   * set kafka consumer config `client.id`
   * @param clientId string to identify client request
   * @return new KafkaConfig based on this but with [[CLIENT_ID]] set to given value
   */
  def withClientId(clientId: String): KafkaConfig = {
    KafkaConfig(config.withValue(CLIENT_ID, configValue(clientId)))
  }

  /**
   * @return kafka consumer config `client.id`
   */
  def getClientId: String = {
    getString(CLIENT_ID)
  }

  /**
   * set max number of messages to output in each `onNext` method of [[org.apache.gearpump.streaming.task.Task]]
   * @param batchSize max number of messages
   * @return new KafkaConfig based on this but with [[CONSUMER_EMIT_BATCH_SIZE]] set to given value
   */
  def withConsumerEmitBatchSize(batchSize: Int): KafkaConfig = {
    KafkaConfig(config.withValue(CONSUMER_EMIT_BATCH_SIZE, configValue(batchSize)))
  }

  /**
   * @return max number of messages to output in each `onNext` method of [[org.apache.gearpump.streaming.task.Task]]
   */
  def getConsumerEmitBatchSize: Int = {
    getInt(CONSUMER_EMIT_BATCH_SIZE)
  }

  /**
   * [[FetchThread]] will sleep for a while if no more messages or
   * the incoming queue size is above the [[FETCH_THRESHOLD]]
   * this is to set sleep interval
   * @param sleepMS sleep interval in milliseconds
   * @return new KafkaConfig based on this but with [[FETCH_SLEEP_MS]] set to given value
   */
  def withFetchSleepMS(sleepMS: Int): KafkaConfig = {
    KafkaConfig(config.withValue(FETCH_SLEEP_MS, configValue(sleepMS)))
  }

  /**
   * [[FetchThread]] will sleep for a while if no more messages or
   * the incoming queue size is above the [[FETCH_THRESHOLD]]
   * this is to get sleep interval
   * @return sleep interval in milliseconds
   */
  def getFetchSleepMS: Int = {
    getInt(FETCH_SLEEP_MS)
  }

  /**
   * [[FetchThread]] stops fetching new messages if its incoming queue
   * size is above the threshold and starts again when the queue size is below it
   * @param threshold queue size
   * @return new KafkaConfig based on this but with [[FETCH_THRESHOLD]] set to give value
   */
  def withFetchThreshold(threshold: Int): KafkaConfig = {
    KafkaConfig(config.withValue(FETCH_THRESHOLD, configValue(threshold)))
  }

  /**
   *
   * [[FetchThread]] stops fetching new messages if its incoming queue
   * size is above the threshold and starts again when the queue size is below it
   * @return fetch threshold
   */
  def getFetchThreshold: Int = {
    getInt(FETCH_THRESHOLD)
  }

  /**
   * set kafka producer topic
   * @param topic topic string
   * @return new KafkaConfig based on this but with [[PRODUCER_TOPIC]] set to given value
   */
  def withProducerTopic(topic: String): KafkaConfig = {
    KafkaConfig(config.withValue(PRODUCER_TOPIC, configValue(topic)))
  }

  /**
   * @return kafka producer topic in string
   */
  def getProducerTopic: String = {
    getString(PRODUCER_TOPIC)
  }

  /**
   * set kafka producer config `request.required.acks`
   * @param acks controls when a produce request is considered complete
   * @return new KafkaConfig based on this but with [[PRODUCER_ACKS]] set to given value
   */
  def withProducerAcks(acks: String): KafkaConfig = {
    KafkaConfig(config.withValue(PRODUCER_ACKS, configValue(acks)))
  }

  /**
   * @return kafka producer config `request.required.acks`
   */
  def getProducerAcks: String = {
    getString(PRODUCER_ACKS)
  }

  /**
   * set kafka producer config `bootstrap.servers`
   * @param servers comma-separated string of server list
   * @return new KafkaConfig based on this but with [[PRODUCER_BOOTSTRAP_SERVERS]] set to given value
   */
  def withProducerBootstrapServers(servers: String): KafkaConfig = {
    KafkaConfig(config.withValue(PRODUCER_BOOTSTRAP_SERVERS, configValue(servers)))
  }

  /**
   * @return kafka producer config `bootstrap.servers`
   */
  def getProducerBootstrapServers: String = {
    getString(PRODUCER_BOOTSTRAP_SERVERS)
  }

  /**
   * set kafka producer config `buffer.memory`
   * @param size buffer memory size
   * @return new KafkaConfig based on this but with [[PRODUCER_BUFFER_MEMORY]] set to given value
   */
  def withProducerBufferMemory(size: Long): KafkaConfig = {
    KafkaConfig(config.withValue(PRODUCER_BUFFER_MEMORY, configValue(size)))
  }

  /**
   * @return kafka producer config `buffer.memory`
   */
  def getProducerBufferMemory: Long = {
    getLong(PRODUCER_BUFFER_MEMORY)
  }

  /**
   * set kafka producer config `compression.type`
   * @param compression compression type
   * @return new KafkaConfig based on this but with [[PRODUCER_COMPRESSION_TYPE]] set to given value
   */
  def withProducerCompressionType(compression: String): KafkaConfig = {
    KafkaConfig(config.withValue(PRODUCER_COMPRESSION_TYPE, configValue(compression)))
  }

  /**
   * @return kafka producer config `compression.type`
   */
  def getProducerCompressionType: String = {
    getString(PRODUCER_COMPRESSION_TYPE)
  }

  /**
   * set kafka producer config `batch.size`
   * @param size number of messages to batch
   * @return new KafkaConfig based on this but with [[PRODUCER_BATCH_SIZE]] set to given value
   */
  def withProducerBatchSize(size: Int): KafkaConfig = {
    KafkaConfig(config.withValue(PRODUCER_BATCH_SIZE, configValue(size)))
  }

  /**
   * @return get kafka producer config `batch.size`
   */
  def getProducerBatchSize: Int = {
    getInt(PRODUCER_BATCH_SIZE)
  }

  /**
   * set kafka producer config `retries`
   * @param retries times to resend messages
   * @return new KafkaConfig based on this but with [[PRODUCER_RETRIES]] set to given value
   */
  def withProducerRetries(retries: Int): KafkaConfig = {
    KafkaConfig(config.withValue(PRODUCER_RETRIES, configValue(retries)))
  }

  /**
   * @return kafka producer config `retries`
   */
  def getProducerRetries: Int = {
    getInt(PRODUCER_RETRIES)
  }

  /**
   * set [[KafkaGrouperFactory]], whose corresponding [[org.apache.gearpump.streaming.kafka.lib.grouper.KafkaGrouper]]
   * defines how [[kafka.common.TopicAndPartition]]s are mapped to source tasks
   *
   * @param className name of the factory class
   * @return new KafkaConfig based on this but with [[GROUPER_FACTORY_CLASS]] set to given value
   */
  def withGrouperFactory(className: String): KafkaConfig = {
    KafkaConfig(config.withValue(GROUPER_FACTORY_CLASS, configValue(className)))
  }

  /**
   * get [[KafkaGrouperFactory]] instance, whose corresponding [[org.apache.gearpump.streaming.kafka.lib.grouper.KafkaGrouper]]
   * defines how [[kafka.common.TopicAndPartition]]s are mapped to source tasks
   * @return
   */
  def getGrouperFactory: KafkaGrouperFactory = {
    getInstance[KafkaGrouperFactory](GROUPER_FACTORY_CLASS)
  }

  /**
   * set number of kafka broker replicas to persist the mapping of (offset, timestamp)
   * @param num number of replicas
   * @return new KafkaConfig based on this but with [[STORAGE_REPLICAS]] set to given value
   */
  def withStorageReplicas(num: Int): KafkaConfig = {
    KafkaConfig(config.withValue(STORAGE_REPLICAS, configValue(num)))
  }

  /**
   * @return number of kafka broker replicas to persist the mapping of (offset, timestamp)
   */
  def getStorageReplicas: Int = {
    getInt(STORAGE_REPLICAS)
  }

  /**
   * set [[MessageDecoder]] to convert kafka raw bytes into gearpump [[org.apache.gearpump.Message]]
   * @param className name of decoder class
   * @return new KafkaConfig based on this but with [[MESSAGE_DECODER_CLASS]] set to given value
   */
  def withMessageDecoder(className: String): KafkaConfig = {
    KafkaConfig(config.withValue(MESSAGE_DECODER_CLASS, configValue(className)))
  }

  /**
   * @return set [[MessageDecoder]] instance to convert kafka raw bytes into gearpump [[org.apache.gearpump.Message]]
   */
  def getMessageDecoder: MessageDecoder = {
    getInstance[MessageDecoder](MESSAGE_DECODER_CLASS)
  }

  /**
   * set [[TimeStampFilter]] to filter gearpump [[org.apache.gearpump.Message]] based on timestamp
   * @param className name of filter class
   * @return new KafkaConfig based on this but with [[TIMESTAMP_FILTER_CLASS]] set to given value
   */
  def withTimeStampFilter(className: String): KafkaConfig = {
    KafkaConfig(config.withValue(TIMESTAMP_FILTER_CLASS, configValue(className)))
  }

  /**
   * @return [[TimeStampFilter]] instance to filter gearpump [[org.apache.gearpump.Message]] based on timestamp
   */
  def getTimeStampFilter: TimeStampFilter = {
    getInstance[TimeStampFilter](TIMESTAMP_FILTER_CLASS)
  }

}

