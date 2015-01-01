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

package org.apache.gearpump.streaming.transaction.lib.kafka

import kafka.api.{FetchRequestBuilder, OffsetRequest}
import kafka.common.ErrorMapping._
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndOffset
import kafka.utils.Utils

/**
 * iterator over kafka SimpleConsumer
 */
class MessageIterator(host: String,
                      port: Int,
                      topic: String,
                      partition: Int,
                      soTimeout: Int,
                      soBufferSize: Int,
                      fetchSize: Int,
                      clientId: String) {
  private val consumer = new SimpleConsumer(host, port, soTimeout, soBufferSize, clientId)
  private var startOffset = consumer.earliestOrLatestOffset(TopicAndPartition(topic, partition),
    OffsetRequest.EarliestTime, -1)
  private var iter = iterator(startOffset)
  private var readMessages = 0L
  private var nextOffset = startOffset
  private var endOffset: Option[Long] = None

  def setStartEndOffsets(startOffset: Long, endOffset: Option[Long]): Unit = {
    this.startOffset = startOffset
    this.endOffset = endOffset
  }

  def next: (Long, Option[Array[Byte]], Array[Byte]) = {
    val mo = iter.next()
    val message = mo.message

    readMessages += 1
    nextOffset = mo.nextOffset

    val offset = mo.offset
    val payload = Utils.readBytes(message.payload)
    (offset, Option(message.key).map(Utils.readBytes), payload)
  }

  @annotation.tailrec
  final def hasNext: Boolean = {
    if (endOffset.isDefined && nextOffset > endOffset.get) {
      close()
      false
    } else if (iter.hasNext) {
      true
    } else if (0 == readMessages) {
      close()
      false
    } else {
      // next fetch
      iter = iterator(nextOffset)
      readMessages = 0
      hasNext
    }
  }

  def close(): Unit = {
    consumer.close()
  }

  private def iterator(offset: Long): Iterator[MessageAndOffset] = {
    val request = new FetchRequestBuilder()
      .clientId(clientId)
      .addFetch(topic, partition, offset, fetchSize)
      .build()

    val response = consumer.fetch(request)
    response.errorCode(topic, partition) match {
      case NoError => response.messageSet(topic, partition).iterator
      case error => throw exceptionFor(error)
    }
  }
}
