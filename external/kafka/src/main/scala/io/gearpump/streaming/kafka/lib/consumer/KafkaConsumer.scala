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

import io.gearpump.streaming.kafka.lib.KafkaUtil
import kafka.api.{FetchRequestBuilder, OffsetRequest}
import kafka.common.ErrorMapping._
import kafka.common.TopicAndPartition
import kafka.consumer.{ConsumerConfig, SimpleConsumer}
import kafka.message.MessageAndOffset
import kafka.utils.Utils

object KafkaConsumer {
  def apply(topic: String, partition: Int, startOffsetTime: Long, config: ConsumerConfig): KafkaConsumer = {
    val connectZk = KafkaUtil.connectZookeeper(config)
    val broker = KafkaUtil.getBroker(connectZk(), topic, partition)
    val soTimeout = config.socketTimeoutMs
    val soBufferSize = config.socketReceiveBufferBytes
    val fetchSize = config.fetchMessageMaxBytes
    val clientId = config.clientId
    val consumer = new SimpleConsumer(broker.host, broker.port, soTimeout, soBufferSize, clientId)
    val getIterator = (offset: Long) => {
      val request = new FetchRequestBuilder()
        .addFetch(topic, partition, offset, fetchSize)
        .build()

    val response = consumer.fetch(request)
      response.errorCode(topic, partition) match {
        case NoError => response.messageSet(topic, partition).iterator
        case error => throw exceptionFor(error)
      }
    }
    new KafkaConsumer(consumer, topic, partition, getIterator, startOffsetTime)
  }
}

/**
 * uses kafka kafka.consumer.SimpleConsumer to consume and iterate over messages from a kafka kafka.common.TopicAndPartition.
 */
class KafkaConsumer(consumer: SimpleConsumer,
                    topic: String,
                    partition: Int,
                    getIterator: (Long) => Iterator[MessageAndOffset],
                    startOffsetTime: Long = OffsetRequest.EarliestTime) {
  private val earliestOffset = consumer
    .earliestOrLatestOffset(TopicAndPartition(topic, partition), startOffsetTime, -1)
  private var nextOffset: Long = earliestOffset
  private var iterator: Iterator[MessageAndOffset] = getIterator(nextOffset)

  def setStartOffset(startOffset: Long): Unit = {
    nextOffset = startOffset
    iterator = getIterator(nextOffset)
  }

  def next(): KafkaMessage = {
    val mo = iterator.next()
    val message = mo.message

    nextOffset = mo.nextOffset

    val offset = mo.offset
    val payload = Utils.readBytes(message.payload)
    new KafkaMessage(topic, partition, offset, Option(message.key).map(Utils.readBytes), payload)
  }

  def hasNext: Boolean = {
    @annotation.tailrec
    def hasNextHelper(iter: Iterator[MessageAndOffset], newIterator: Boolean): Boolean = {
      if (iter.hasNext) true
      else if (newIterator) false
      else {
        iterator = getIterator(nextOffset)
        hasNextHelper(iterator, newIterator = true)
      }
    }
    hasNextHelper(iterator, newIterator = false)
  }

  def getNextOffset: Long = nextOffset

  def close(): Unit = {
    consumer.close()
  }


}
