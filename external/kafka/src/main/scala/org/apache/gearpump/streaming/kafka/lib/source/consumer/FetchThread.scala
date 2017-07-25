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

package org.apache.gearpump.streaming.kafka.lib.source.consumer

import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.LinkedBlockingQueue

import kafka.common.TopicAndPartition
import org.apache.gearpump.streaming.kafka.lib.util.KafkaClient
import org.apache.gearpump.streaming.kafka.util.KafkaConfig
import org.slf4j.Logger

import org.apache.gearpump.util.LogUtil

object FetchThread {
  private val LOG: Logger = LogUtil.getLogger(classOf[FetchThread])

  val factory = new FetchThreadFactory

  class FetchThreadFactory extends java.io.Serializable {
    def getFetchThread(config: KafkaConfig, client: KafkaClient): FetchThread = {
      val fetchThreshold = config.getInt(KafkaConfig.FETCH_THRESHOLD_CONFIG)
      val fetchSleepMS = config.getLong(KafkaConfig.FETCH_SLEEP_MS_CONFIG)
      val startOffsetTime = config.getLong(KafkaConfig.CONSUMER_START_OFFSET_CONFIG)
      FetchThread(fetchThreshold, fetchSleepMS, startOffsetTime, client)
    }
  }

  def apply(fetchThreshold: Int,
      fetchSleepMS: Long,
      startOffsetTime: Long,
      client: KafkaClient): FetchThread = {
    val createConsumer = (tp: TopicAndPartition) =>
      client.createConsumer(tp.topic, tp.partition, startOffsetTime)
    val incomingQueue = new LinkedBlockingQueue[KafkaMessage]()
    val sleeper = new ExponentialBackoffSleeper(
      backOffMultiplier = 2.0,
      initialDurationMs = 100L,
      maximumDurationMs = 10000L)
    new FetchThread(createConsumer, incomingQueue, sleeper, fetchThreshold, fetchSleepMS)
  }
}

/**
 * A thread to fetch messages from multiple kafka org.apache.kafka.TopicAndPartition and puts them
 * onto a queue, which is asynchronously polled by a consumer
 *
 * @param createConsumer given a org.apache.kafka.TopicAndPartition, create a
 *                       [[KafkaConsumer]] to
 *                       connect to it
 * @param incomingQueue a queue to buffer incoming messages
 * @param fetchThreshold above which thread should stop fetching messages
 * @param fetchSleepMS interval to sleep when no more messages or hitting fetchThreshold
 */
private[kafka] class FetchThread(
    createConsumer: TopicAndPartition => KafkaConsumer,
    incomingQueue: LinkedBlockingQueue[KafkaMessage],
    sleeper: ExponentialBackoffSleeper,
    fetchThreshold: Int,
    fetchSleepMS: Long) extends Thread {
  import org.apache.gearpump.streaming.kafka.lib.source.consumer.FetchThread._

  private var consumers: Map[TopicAndPartition, KafkaConsumer] =
    Map.empty[TopicAndPartition, KafkaConsumer]
  private var topicAndPartitions: Array[TopicAndPartition] =
    Array.empty[TopicAndPartition]
  private var nextOffsets = Map.empty[TopicAndPartition, Long]
  private var reset = false

  def setTopicAndPartitions(topicAndPartitions: Array[TopicAndPartition]): Unit = {
    this.topicAndPartitions = topicAndPartitions
    consumers = createAllConsumers
  }

  def setStartOffset(tp: TopicAndPartition, startOffset: Long): Unit = {
    consumers.get(tp).foreach(_.setStartOffset(startOffset))
  }

  def poll: Option[KafkaMessage] = {
    Option(incomingQueue.poll())
  }

  override def run(): Unit = {
    try {
      while (!Thread.currentThread().isInterrupted) {
        runLoop()
      }
    } catch {
      case e: InterruptedException => LOG.info("fetch thread got interrupted exception")
      case e: ClosedByInterruptException => LOG.info("fetch thread closed by interrupt exception")
    } finally {
      consumers.values.foreach(_.close())
    }
  }

  private[lib] def runLoop(): Unit = {
    try {
      if (reset) {
        nextOffsets = consumers.mapValues(_.getNextOffset)
        resetConsumers(nextOffsets)
        reset = false
      }
      val fetchMore: Boolean = fetchMessage
      sleeper.reset()
      if (!fetchMore) {
        // sleep for given duration
        sleeper.sleep(fetchSleepMS)
      }
    } catch {
      case exception: Exception =>
        LOG.warn(s"resetting consumers due to $exception")
        reset = true
        // sleep for exponentially increasing duration
        sleeper.sleep()
    }
  }

  /**
   * fetch message from each TopicAndPartition in a round-robin way
   *
   * @return whether to fetch more messages
   */
  private def fetchMessage: Boolean = {
    if (incomingQueue.size >= fetchThreshold) {
      false
    } else {
      consumers.foldLeft(false) { (hasNext, tpAndConsumer) =>
        val (_, consumer) = tpAndConsumer
        if (consumer.hasNext) {
          incomingQueue.put(consumer.next())
          true
        } else {
          hasNext
        }
      }
    }
  }

  private def createAllConsumers: Map[TopicAndPartition, KafkaConsumer] = {
    topicAndPartitions.map(tp => tp -> createConsumer(tp)).toMap
  }

  private def resetConsumers(nextOffsets: Map[TopicAndPartition, Long]): Unit = {
    consumers.values.foreach(_.close())
    consumers = createAllConsumers
    consumers.foreach { case (tp, consumer) =>
      consumer.setStartOffset(nextOffsets(tp))
    }
  }
}
