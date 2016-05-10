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

package org.apache.gearpump.streaming.kafka.lib.consumer

import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.LinkedBlockingQueue

import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import org.slf4j.Logger

import org.apache.gearpump.util.LogUtil

object FetchThread {
  private val LOG: Logger = LogUtil.getLogger(classOf[FetchThread])

  def apply(topicAndPartitions: Array[TopicAndPartition],
      fetchThreshold: Int,
      fetchSleepMS: Long,
      startOffsetTime: Long,
      consumerConfig: ConsumerConfig): FetchThread = {
    val createConsumer = (tp: TopicAndPartition) =>
      KafkaConsumer(tp.topic, tp.partition, startOffsetTime, consumerConfig)

    val incomingQueue = new LinkedBlockingQueue[KafkaMessage]()
    new FetchThread(topicAndPartitions, createConsumer, incomingQueue, fetchThreshold, fetchSleepMS)
  }
}

/**
 * A thread to fetch messages from multiple kafka org.apache.kafka.TopicAndPartition and puts them
 * onto a queue, which is asynchronously polled by a consumer
 *
 * @param createConsumer given a org.apache.kafka.TopicAndPartition, create a
 *                       [[org.apache.gearpump.streaming.kafka.lib.consumer.KafkaConsumer]] to
 *                       connect to it
 * @param incomingQueue a queue to buffer incoming messages
 * @param fetchThreshold above which thread should stop fetching messages
 * @param fetchSleepMS interval to sleep when no more messages or hitting fetchThreshold
 */
private[kafka] class FetchThread(topicAndPartitions: Array[TopicAndPartition],
    createConsumer: TopicAndPartition => KafkaConsumer,
    incomingQueue: LinkedBlockingQueue[KafkaMessage],
    fetchThreshold: Int,
    fetchSleepMS: Long) extends Thread {
  import org.apache.gearpump.streaming.kafka.lib.consumer.FetchThread._

  private var consumers: Map[TopicAndPartition, KafkaConsumer] = createAllConsumers

  def setStartOffset(tp: TopicAndPartition, startOffset: Long): Unit = {
    consumers(tp).setStartOffset(startOffset)
  }

  def poll: Option[KafkaMessage] = {
    Option(incomingQueue.poll())
  }

  override def run(): Unit = {
    try {
      var nextOffsets = Map.empty[TopicAndPartition, Long]
      var reset = false
      val sleeper = new ExponentialBackoffSleeper(
        backOffMultiplier = 2.0,
        initialDurationMs = 100L,
        maximumDurationMs = 10000L)
      while (!Thread.currentThread().isInterrupted) {
        try {
          if (reset) {
            nextOffsets = consumers.mapValues(_.getNextOffset)
            resetConsumers(nextOffsets)
            reset = false
          }
          val hasMoreMessages = fetchMessage
          sleeper.reset()
          if (!hasMoreMessages) {
            Thread.sleep(fetchSleepMS)
          }
        } catch {
          case exception: Exception =>
            LOG.warn(s"resetting consumers due to $exception")
            reset = true
            sleeper.sleep()
        }
      }
    } catch {
      case e: InterruptedException => LOG.info("fetch thread got interrupted exception")
      case e: ClosedByInterruptException => LOG.info("fetch thread closed by interrupt exception")
    } finally {
      consumers.values.foreach(_.close())
    }
  }

  /**
   * fetch message from each TopicAndPartition in a round-robin way
   */
  def fetchMessage: Boolean = {
    consumers.foldLeft(false) { (hasNext, tpAndConsumer) =>
      val (_, consumer) = tpAndConsumer
      if (incomingQueue.size < fetchThreshold) {
        if (consumer.hasNext) {
          incomingQueue.put(consumer.next())
          true
        } else {
          hasNext
        }
      } else {
        true
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
