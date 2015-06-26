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

import com.twitter.bijection.Injection
import com.typesafe.config.{Config, ConfigFactory, ConfigValue, ConfigValueFactory}
import org.apache.gearpump.streaming.kafka.lib.grouper.KafkaGrouperFactory
import org.apache.gearpump.streaming.transaction.api.{MessageDecoder, TimeStampFilter}
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.util.Try

object KafkaConfig {

  val NAME = "kafka_config"

  val CONSUMER_TOPICS = "kafka.consumer.topics"
  val CONSUMER_EMIT_BATCH_SIZE = "kafka.consumer.emit.batch.size"
  val FETCH_THRESHOLD = "kafka.consumer.fetch.threshold"
  val FETCH_SLEEP_MS = "kafka.consumer.fetch.sleep.ms"
  val PRODUCER_TOPIC = "kafka.producer.topic"
  val STORAGE_REPLICAS = "kafka.storage.replicas"
  val GROUPER_FACTORY_CLASS = "kafka.grouper.factory.class"
  val MESSAGE_DECODER_CLASS = "kafka.task.message.decoder.class"
  val TIMESTAMP_FILTER_CLASS = "kafka.task.timestamp.filter.class"

  def apply(gearpumpConfig: Config, consumerConfig: Properties, producerConfig: Properties): KafkaConfig = {
    new KafkaConfig(gearpumpConfig, consumerConfig, producerConfig)
  }

  private val LOG: Logger = LogUtil.getLogger(getClass)
}

class KafkaConfig(gearpumpConfig: Config = ConfigFactory.parseResources("kafka.conf"),
                  val consumerConfig: Properties = KafkaUtil.loadProperties("consumer.properties"),
                  val producerConfig: Properties = KafkaUtil.loadProperties("producer.properties")) extends Serializable  {
  import org.apache.gearpump.streaming.kafka.lib.KafkaConfig._

  if (!consumerConfig.containsKey("zookeeper.connect")) {
    consumerConfig.setProperty("zookeeper.connect", "localhost:2181")
  }

  if (!consumerConfig.containsKey("group.id")) {
    consumerConfig.setProperty("group.id", "")
  }

  if (!producerConfig.containsKey("bootstrap.servers")) {
    producerConfig.setProperty("bootstrap.servers", "localhost:9092")
  }

  private def get[T](key: String, optValue: Option[T], defaultValue: Option[T] = None): T = {
    val value = optValue orElse defaultValue orElse (throw new RuntimeException(s"$key not found"))
    value.get
  }

  private def getString(key: String, defaultValue: Option[String] = None): String = {
    get[String](key, Try(gearpumpConfig.getString(key)).toOption, defaultValue)
  }

  private def getInt(key: String, defaultValue: Option[Int] = None): Int = {
    get[Int](key, Try(gearpumpConfig.getInt(key)).toOption, defaultValue)
  }

  private def getLong(key: String, defaultValue: Option[Long] = None): Long = {
    get[Long](key, Try(gearpumpConfig.getLong(key)).toOption, defaultValue)
  }

  private def getInstance[C](key: String, defaultValue: Option[String] = None): C = {
    Class.forName(getString(key, defaultValue)).newInstance().asInstanceOf[C]
  }

  private def configValue(anyRef: AnyRef): ConfigValue = {
    ConfigValueFactory.fromAnyRef(anyRef)
  }

  private def copy(config: Config): KafkaConfig = {
    KafkaConfig(gearpumpConfig = config, consumerConfig, producerConfig)
  }

  def withConsumerConfig(config: Properties): KafkaConfig = {
    KafkaConfig(gearpumpConfig, consumerConfig = config, producerConfig)
  }

  def withProducerConfig(config: Properties): KafkaConfig = {
    KafkaConfig(gearpumpConfig, consumerConfig, producerConfig = config)
  }
  
  /**
   * set kafka consumer topics
   * @param topics comma-separated string
   * @return new KafkaConfig based on this but with [[CONSUMER_TOPICS]] set to given value
   */
  def withConsumerTopics(topics: String): KafkaConfig = {
    copy(gearpumpConfig.withValue(CONSUMER_TOPICS, configValue(topics)))
  }

  /**
   * @return a list of kafka consumer topics
   */
  def getConsumerTopics: List[String] = {
    getString(CONSUMER_TOPICS).split(",").toList
  }


  /**
   * set max number of messages to output in each `onNext` method of [[org.apache.gearpump.streaming.task.Task]]
   * @param batchSize max number of messages
   * @return new KafkaConfig based on this but with [[CONSUMER_EMIT_BATCH_SIZE]] set to given value
   */
  def withConsumerEmitBatchSize(batchSize: Int): KafkaConfig = {
    copy(gearpumpConfig.withValue(CONSUMER_EMIT_BATCH_SIZE,
      configValue(Injection[Int, java.lang.Integer](batchSize))))
  }

  /**
   * @return max number of messages to output in each `onNext` method of [[org.apache.gearpump.streaming.task.Task]]
   */
  def getConsumerEmitBatchSize: Int = {
    getInt(CONSUMER_EMIT_BATCH_SIZE)
  }

  /**
   * [[org.apache.gearpump.streaming.kafka.lib.consumer.FetchThread]] will sleep for a while if no more messages or
   * the incoming queue size is above the [[FETCH_THRESHOLD]]
   * this is to set sleep interval
   * @param sleepMS sleep interval in milliseconds
   * @return new KafkaConfig based on this but with [[FETCH_SLEEP_MS]] set to given value
   */
  def withFetchSleepMS(sleepMS: Int): KafkaConfig = {
    copy(gearpumpConfig.withValue(FETCH_SLEEP_MS,
      configValue(Injection[Int, java.lang.Integer](sleepMS))))
  }

  /**
   * [[org.apache.gearpump.streaming.kafka.lib.consumer.FetchThread]] will sleep for a while if no more messages or
   * the incoming queue size is above the [[FETCH_THRESHOLD]]
   * this is to get sleep interval
   * @return sleep interval in milliseconds
   */
  def getFetchSleepMS: Int = {
    getInt(FETCH_SLEEP_MS)
  }

  /**
   * [[org.apache.gearpump.streaming.kafka.lib.consumer.FetchThread]] stops fetching new messages if its incoming queue
   * size is above the threshold and starts again when the queue size is below it
   * @param threshold queue size
   * @return new KafkaConfig based on this but with [[FETCH_THRESHOLD]] set to give value
   */
  def withFetchThreshold(threshold: Int): KafkaConfig = {
    copy(gearpumpConfig.withValue(FETCH_THRESHOLD,
      configValue(Injection[Int, java.lang.Integer](threshold))))
  }

  /**
   *
   * [[org.apache.gearpump.streaming.kafka.lib.consumer.FetchThread]] stops fetching new messages if its incoming queue
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
    copy(gearpumpConfig.withValue(PRODUCER_TOPIC, configValue(topic)))
  }

  /**
   * @return kafka producer topic in string
   */
  def getProducerTopic: String = {
    getString(PRODUCER_TOPIC)
  }

  /**
   * set [[KafkaGrouperFactory]], whose corresponding [[org.apache.gearpump.streaming.kafka.lib.grouper.KafkaGrouper]]
   * defines how [[kafka.common.TopicAndPartition]]s are mapped to source tasks
   *
   * @param className name of the factory class
   * @return new KafkaConfig based on this but with [[GROUPER_FACTORY_CLASS]] set to given value
   */
  def withGrouperFactory(className: String): KafkaConfig = {
    copy(gearpumpConfig.withValue(GROUPER_FACTORY_CLASS, configValue(className)))
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
    copy(gearpumpConfig.withValue(STORAGE_REPLICAS,
      configValue(Injection[Int, java.lang.Integer](num))))
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
    copy(gearpumpConfig.withValue(MESSAGE_DECODER_CLASS, configValue(className)))
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
    copy(gearpumpConfig.withValue(TIMESTAMP_FILTER_CLASS, configValue(className)))
  }

  /**
   * @return [[TimeStampFilter]] instance to filter gearpump [[org.apache.gearpump.Message]] based on timestamp
   */
  def getTimeStampFilter: TimeStampFilter = {
    getInstance[TimeStampFilter](TIMESTAMP_FILTER_CLASS)
  }

}

