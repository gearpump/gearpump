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

package io.gearpump.streaming.kafka.lib.consumer

import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.LinkedBlockingQueue

import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import io.gearpump.util.LogUtil
import org.slf4j.Logger

object FetchThread {
  private val LOG: Logger = LogUtil.getLogger(classOf[FetchThread])

  def apply(topicAndPartitions: Array[TopicAndPartition],
            fetchThreshold: Int,
            fetchSleepMS: Long,
            startOffsetTime: Long,
            consumerConfig: ConsumerConfig): FetchThread = {
    val consumers: Map[TopicAndPartition, KafkaConsumer] = topicAndPartitions.map {
      tp =>
        tp -> KafkaConsumer(tp.topic, tp.partition, startOffsetTime, consumerConfig)
    }.toMap
    val incomingQueue = new LinkedBlockingQueue[KafkaMessage]()
    new FetchThread(consumers, incomingQueue, fetchThreshold, fetchSleepMS)
  }
}

/**
 * A thread to fetch messages from multiple kafka [[TopicAndPartition]]s and puts them
 * onto a queue, which is asynchronously polled by a consumer
 * @param consumers [[KafkaConsumer]]s by kafka [[TopicAndPartition]]s
 * @param incomingQueue a queue to buffer incoming messages
 * @param fetchThreshold above which thread should stop fetching messages
 * @param fetchSleepMS interval to sleep when no more messages or hitting fetchThreshold
 */
private[kafka] class FetchThread(consumers: Map[TopicAndPartition, KafkaConsumer],
                                 incomingQueue: LinkedBlockingQueue[KafkaMessage],
                                 fetchThreshold: Int,
                                 fetchSleepMS: Long) extends Thread {
  import FetchThread._

  def setStartOffset(tp: TopicAndPartition, startOffset: Long): Unit = {
    consumers(tp).setStartOffset(startOffset)
  }

  def poll: Option[KafkaMessage] = {
    Option(incomingQueue.poll())
  }

  override def run(): Unit = {
    try {
      while (!Thread.currentThread.isInterrupted) {
        val hasMoreMessages = fetchMessage
        if (!hasMoreMessages || incomingQueue.size >= fetchThreshold) {
          Thread.sleep(fetchSleepMS)
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
}
