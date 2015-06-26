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
import org.apache.gearpump.streaming.kafka.lib.grouper.{KafkaDefaultGrouperFactory, KafkaGrouperFactory}
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.util.Try

object KafkaConfig {

  val NAME = "kafka_config"

  val ZOOKEEPER_CONNECT = "zookeeper.connect"
  val GROUP_ID = "group.id"
  val BOOTSTRAP_SERVERS = "bootstrap.servers"
  val CONSUMER_TOPICS = "kafka.consumer.topics"
  val FETCH_THRESHOLD = "kafka.consumer.fetch.threshold"
  val FETCH_SLEEP_MS = "kafka.consumer.fetch.sleep.ms"
  val PRODUCER_TOPIC = "kafka.producer.topic"
  val STORAGE_REPLICAS = "kafka.storage.replicas"
  val GROUPER_FACTORY_CLASS = "kafka.grouper.factory.class"

  def load(file: String): KafkaConfig = {
    new KafkaConfig(ConfigFactory.parseResources(file))
  }

  def empty(): KafkaConfig = {
    new KafkaConfig()
  }

  def apply(gearpumpConfig: Config, consumerConfig: Properties, producerConfig: Properties): KafkaConfig = {
    new KafkaConfig(gearpumpConfig, consumerConfig, producerConfig)
  }

  private val LOG: Logger = LogUtil.getLogger(getClass)
}

/**
 * This is a utility class to make configuring kafka easier for users
 * kafka configuration is comprised of three parts
 *   1. gearpump configuration for kafka
 *   2. kafka consumer configuration
 *   3. kafka producer configuration
 *
 * For 1, users could load from a HOCON file or through `with` API.
 * For 2 and 3, users could load from kafka standard properties files or put configurations into
 * `consumerConfig` and `producerConfig` directly since they are mutable. We also set default values
 * for required configurations if not provided
 *
 * please refer to http://kafka.apache.org/documentation.html for more details of kafka configs
 *
 *
 * @param gearpumpConfig gearpump config for kafka
 * @param consumerConfig kafka consumer config
 * @param producerConfig kafka producer config
 */
class KafkaConfig(gearpumpConfig: Config = ConfigFactory.empty(),
                  val consumerConfig: Properties = new Properties,
                  val producerConfig: Properties = new Properties) extends Serializable  {
  import org.apache.gearpump.streaming.kafka.lib.KafkaConfig._

  if (!consumerConfig.containsKey(ZOOKEEPER_CONNECT)) {
    consumerConfig.setProperty(ZOOKEEPER_CONNECT, "localhost:2181")
  }

  if (!consumerConfig.containsKey(GROUP_ID)) {
    consumerConfig.setProperty(GROUP_ID, "")
  }

  if (!producerConfig.containsKey(BOOTSTRAP_SERVERS)) {
    producerConfig.setProperty(BOOTSTRAP_SERVERS, "localhost:9092")
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
    getString(CONSUMER_TOPICS, Some("topic1")).split(",").toList
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
    getInt(FETCH_SLEEP_MS, Some(100))
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
    getInt(FETCH_THRESHOLD, Some(5000))
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
    getString(PRODUCER_TOPIC, Some("topic2"))
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
    getInstance[KafkaGrouperFactory](GROUPER_FACTORY_CLASS, Some(classOf[KafkaDefaultGrouperFactory].getName))
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
    getInt(STORAGE_REPLICAS, Some(1))
  }
}

