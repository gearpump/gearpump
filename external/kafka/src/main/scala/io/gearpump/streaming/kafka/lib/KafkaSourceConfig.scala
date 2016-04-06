/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.streaming.kafka.lib

import java.util.Properties

import kafka.api.OffsetRequest
import kafka.consumer.ConsumerConfig
import org.slf4j.Logger

import io.gearpump.streaming.kafka.KafkaSource
import io.gearpump.streaming.kafka.lib.grouper.{KafkaDefaultGrouper, KafkaGrouper}
import io.gearpump.util.LogUtil

object KafkaSourceConfig {

  val NAME = "kafka_config"

  val ZOOKEEPER_CONNECT = "zookeeper.connect"
  val GROUP_ID = "group.id"
  val CONSUMER_START_OFFSET = "kafka.consumer.start.offset"
  val CONSUMER_TOPICS = "kafka.consumer.topics"
  val FETCH_THRESHOLD = "kafka.consumer.fetch.threshold"
  val FETCH_SLEEP_MS = "kafka.consumer.fetch.sleep.ms"
  val GROUPER_CLASS = "kafka.grouper.class"

  private val LOG: Logger = LogUtil.getLogger(getClass)

  def apply(consumerProps: Properties): KafkaSourceConfig = new KafkaSourceConfig(consumerProps)
}

/**
 * Extends kafka.consumer.ConsumerConfig with specific config needed by
 * [[io.gearpump.streaming.kafka.KafkaSource]]
 *
 * @param consumerProps kafka consumer config
 */
class KafkaSourceConfig(val consumerProps: Properties = new Properties)
  extends java.io.Serializable {
  import KafkaSourceConfig._

  if (!consumerProps.containsKey(ZOOKEEPER_CONNECT)) {
    consumerProps.setProperty(ZOOKEEPER_CONNECT, "localhost:2181")
  }

  if (!consumerProps.containsKey(GROUP_ID)) {
    consumerProps.setProperty(GROUP_ID, "gearpump")
  }

  def consumerConfig: ConsumerConfig = new ConsumerConfig(consumerProps)

  /**
   * Set kafka consumer topics, seperated by comma.
   *
   * @param topics comma-separated string
   * @return new KafkaConfig based on this but with
   *         [[io.gearpump.streaming.kafka.lib.KafkaSourceConfig.CONSUMER_TOPICS]]
   *         set to given value
   */
  def withConsumerTopics(topics: String): KafkaSourceConfig = {
    consumerProps.setProperty(CONSUMER_TOPICS, topics)
    KafkaSourceConfig(consumerProps)
  }

  /**
   * Returns a list of kafka consumer topics
   */
  def getConsumerTopics: List[String] = {
    Option(consumerProps.getProperty(CONSUMER_TOPICS)).getOrElse("topic1").split(",").toList
  }

  /**
   * Sets the sleep interval if there are no more message or message buffer is full.
   *
   * Consumer.FetchThread will sleep for a while if no more messages or
   * the incoming queue size is above the
   * [[io.gearpump.streaming.kafka.lib.KafkaSourceConfig.FETCH_THRESHOLD]]
   *
   * @param sleepMS sleep interval in milliseconds
   * @return new KafkaConfig based on this but with
   *         [[io.gearpump.streaming.kafka.lib.KafkaSourceConfig.FETCH_SLEEP_MS]] set to given value
   */
  def withFetchSleepMS(sleepMS: Int): KafkaSourceConfig = {
    consumerProps.setProperty(FETCH_SLEEP_MS, s"$sleepMS")
    KafkaSourceConfig(consumerProps)
  }

  /**
   * Gets the sleep interval
   *
   * Consumer.FetchThread sleeps for a while if no more messages or
   * the incoming queue is full (size is bigger than the
   * [[io.gearpump.streaming.kafka.lib.KafkaSourceConfig.FETCH_THRESHOLD]])
   *
   * @return sleep interval in milliseconds
   */
  def getFetchSleepMS: Int = {
    Option(consumerProps.getProperty(FETCH_SLEEP_MS)).getOrElse("100").toInt
  }

  /**
   * Sets the batch size we use for one fetch.
   *
   * Consumer.FetchThread stops fetching new messages if its incoming queue
   * size is above the threshold and starts again when the queue size is below it
   *
   * @param threshold queue size
   * @return new KafkaConfig based on this but with
   *         [[io.gearpump.streaming.kafka.lib.KafkaSourceConfig.FETCH_THRESHOLD]] set to give value
   */
  def withFetchThreshold(threshold: Int): KafkaSourceConfig = {
    consumerProps.setProperty(FETCH_THRESHOLD, s"$threshold")
    KafkaSourceConfig(consumerProps)
  }

  /**
   * Returns fetch batch size.
   *
   * Consumer.FetchThread stops fetching new messages if
   * its incoming queue size is above the threshold and starts again when the queue size is below it
   *
   * @return fetch threshold
   */
  def getFetchThreshold: Int = {
    Option(consumerProps.getProperty(FETCH_THRESHOLD)).getOrElse("10000").toInt
  }

  /**
   * Sets [[io.gearpump.streaming.kafka.lib.grouper.KafkaGrouper]], which
   * defines how kafka.common.TopicAndPartitions are mapped to source tasks.
   *
   * @param className name of the factory class
   * @return new KafkaConfig based on this but with
   *         [[io.gearpump.streaming.kafka.lib.KafkaSourceConfig.GROUPER_CLASS]] set to given value
   */
  def withGrouper(className: String): KafkaSourceConfig = {
    consumerProps.setProperty(GROUPER_CLASS, className)
    KafkaSourceConfig(consumerProps)
  }

  /**
   * Returns [[io.gearpump.streaming.kafka.lib.grouper.KafkaGrouper]] instance, which
   * defines how kafka.common.TopicAndPartitions are mapped to source tasks
   */
  def getGrouper: KafkaGrouper = {
    Class.forName(Option(consumerProps.getProperty(GROUPER_CLASS))
      .getOrElse(classOf[KafkaDefaultGrouper].getName)).newInstance().asInstanceOf[KafkaGrouper]
  }

  def withConsumerStartOffset(earliestOrLatest: Long): KafkaSourceConfig = {
    consumerProps.setProperty(CONSUMER_START_OFFSET, s"$earliestOrLatest")
    KafkaSourceConfig(consumerProps)
  }

  def getConsumerStartOffset: Long = {
    Option(consumerProps.getProperty(CONSUMER_START_OFFSET))
      .getOrElse(s"${OffsetRequest.EarliestTime}").toLong
  }
}

