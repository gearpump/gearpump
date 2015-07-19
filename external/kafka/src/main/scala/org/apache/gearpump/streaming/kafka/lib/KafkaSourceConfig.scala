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

import kafka.consumer.ConsumerConfig
import org.apache.gearpump.streaming.kafka.lib.grouper.{KafkaDefaultGrouper, KafkaGrouper}
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

object KafkaSourceConfig {

  val NAME = "kafka_config"

  val ZOOKEEPER_CONNECT = "zookeeper.connect"
  val GROUP_ID = "group.id"
  val CONSUMER_TOPICS = "kafka.consumer.topics"
  val FETCH_THRESHOLD = "kafka.consumer.fetch.threshold"
  val FETCH_SLEEP_MS = "kafka.consumer.fetch.sleep.ms"
  val GROUPER_CLASS = "kafka.grouper.class"

  private val LOG: Logger = LogUtil.getLogger(getClass)

  def apply(consumerProps: Properties): KafkaSourceConfig = new KafkaSourceConfig(consumerProps)
}

/**
 * this class extends kafka [[ConsumerConfig]] with specific configs for [[org.apache.gearpump.streaming.kafka.KafkaSource]]
 * @param consumerProps kafka consumer config
 */
class KafkaSourceConfig(val consumerProps: Properties = new Properties) extends java.io.Serializable  {
  import org.apache.gearpump.streaming.kafka.lib.KafkaSourceConfig._

  if (!consumerProps.contains(ZOOKEEPER_CONNECT)) {
    consumerProps.setProperty(ZOOKEEPER_CONNECT, "localhost:2181")
  }

  if (!consumerProps.contains(GROUP_ID)) {
    consumerProps.setProperty(GROUP_ID, "gearpump")
  }

  def consumerConfig: ConsumerConfig = new ConsumerConfig(consumerProps)

  /**
   * set kafka consumer topics
   * @param topics comma-separated string
   * @return new KafkaConfig based on this but with [[CONSUMER_TOPICS]] set to given value
   */
  def withConsumerTopics(topics: String): KafkaSourceConfig = {
    consumerProps.setProperty(CONSUMER_TOPICS, topics)
    KafkaSourceConfig(consumerProps)
  }

  /**
   * @return a list of kafka consumer topics
   */
  def getConsumerTopics: List[String] = {
    Option(consumerProps.getProperty(CONSUMER_TOPICS)).getOrElse("topic1").split(",").toList
  }

  /**
   * [[org.apache.gearpump.streaming.kafka.lib.consumer.FetchThread]] will sleep for a while if no more messages or
   * the incoming queue size is above the [[FETCH_THRESHOLD]]
   * this is to set sleep interval
   * @param sleepMS sleep interval in milliseconds
   * @return new KafkaConfig based on this but with [[FETCH_SLEEP_MS]] set to given value
   */
  def withFetchSleepMS(sleepMS: Int): KafkaSourceConfig = {
    consumerProps.setProperty(FETCH_SLEEP_MS, sleepMS + "")
    KafkaSourceConfig(consumerProps)
  }

  /**
   * [[org.apache.gearpump.streaming.kafka.lib.consumer.FetchThread]] will sleep for a while if no more messages or
   * the incoming queue size is above the [[FETCH_THRESHOLD]]
   * this is to get sleep interval
   * @return sleep interval in milliseconds
   */
  def getFetchSleepMS: Int = {
    Option(consumerProps.getProperty(FETCH_SLEEP_MS)).getOrElse("100").toInt
  }

  /**
   * [[org.apache.gearpump.streaming.kafka.lib.consumer.FetchThread]] stops fetching new messages if its incoming queue
   * size is above the threshold and starts again when the queue size is below it
   * @param threshold queue size
   * @return new KafkaConfig based on this but with [[FETCH_THRESHOLD]] set to give value
   */
  def withFetchThreshold(threshold: Int): KafkaSourceConfig = {
    consumerProps.setProperty(FETCH_THRESHOLD, threshold + "")
    KafkaSourceConfig(consumerProps)
  }

  /**
   *
   * [[org.apache.gearpump.streaming.kafka.lib.consumer.FetchThread]] stops fetching new messages if its incoming queue
   * size is above the threshold and starts again when the queue size is below it
   * @return fetch threshold
   */
  def getFetchThreshold: Int = {
    Option(consumerProps.getProperty(FETCH_THRESHOLD)).getOrElse("10000").toInt
  }

  /**
   * set [[org.apache.gearpump.streaming.kafka.lib.grouper.KafkaGrouper]], whose corresponding [[org.apache.gearpump.streaming.kafka.lib.grouper.KafkaGrouper]]
   * defines how [[kafka.common.TopicAndPartition]]s are mapped to source tasks
   *
   * @param className name of the factory class
   * @return new KafkaConfig based on this but with [[GROUPER_CLASS]] set to given value
   */
  def withGrouper(className: String): KafkaSourceConfig = {
    consumerProps.setProperty(GROUPER_CLASS, className)
    KafkaSourceConfig(consumerProps)
  }

  /**
   * get [[org.apache.gearpump.streaming.kafka.lib.grouper.KafkaGrouper]] instance, whose corresponding [[org.apache.gearpump.streaming.kafka.lib.grouper.KafkaGrouper]]
   * defines how [[kafka.common.TopicAndPartition]]s are mapped to source tasks
   * @return
   */
  def getGrouper: KafkaGrouper = {
    Class.forName(Option(consumerProps.getProperty(GROUPER_CLASS)).getOrElse(classOf[KafkaDefaultGrouper].getName))
        .newInstance().asInstanceOf[KafkaGrouper]
  }
}

