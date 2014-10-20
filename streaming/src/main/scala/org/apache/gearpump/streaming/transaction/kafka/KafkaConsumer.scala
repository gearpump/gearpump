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

package org.apache.gearpump.streaming.transaction.kafka

import kafka.api.{FetchRequestBuilder, OffsetRequest}
import kafka.common.ErrorMapping._
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndOffset
import kafka.utils.{Utils, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.slf4j.{Logger, LoggerFactory}
import java.util.concurrent.LinkedBlockingQueue
import scala.util.{Try, Success, Failure}


case class KafkaMessage(topicAndPartition: TopicAndPartition, offset: Long,
                        key: Array[Byte], msg: Array[Byte])

object KafkaConsumer {

  // TODO: use kafka.cluster.Broker once it is not package private in 0.8.2
  object Broker {
    def toString(brokers: List[Broker]) = brokers.mkString(",")
  }

  case class Broker(host: String, port: Int) {
    override def toString = s"${host}:${port}"
  }

  private val LOG: Logger = LoggerFactory.getLogger(classOf[KafkaConsumer])
}


class KafkaConsumer(topicAndPartitions: Array[TopicAndPartition],
                    clientId: String, socketTimeout: Int,
                    receiveBufferSize: Int, fetchSize: Int,
                    zkClient: ZkClient)  {
  import org.apache.gearpump.streaming.transaction.kafka.KafkaConsumer._

  private val leaders: Map[TopicAndPartition, Broker] = topicAndPartitions.map {
    tp =>
      val topic = tp.topic
      val partition = tp.partition
       val leader =  ZkUtils.getLeaderForPartition(zkClient, topic, partition)
        .getOrElse(throw new Exception(s"leader not available for TopicAndPartition(${tp.topic}, ${tp.partition})"))
      val broker = ZkUtils.getBrokerInfo(zkClient, leader)
        .getOrElse(throw new Exception(s"broker info not found for leader ${leader}"))
      tp -> Broker(broker.host, broker.port)
  }.toMap

  private val iterators: Map[TopicAndPartition, MessageIterator] = topicAndPartitions.map(
    tp => {
      val broker = leaders(tp)
      val host = broker.host
      val port = broker.port
      (tp, new MessageIterator(host, port, tp.topic, tp.partition,
        socketTimeout, receiveBufferSize, fetchSize, clientId))
    }).toMap

  private var partitionIndex = 0
  private val partitionNum = topicAndPartitions.length

  private var noMessages: Set[TopicAndPartition] = Set.empty[TopicAndPartition]
  private val noMessageSleepMS = 100
  private val incomingQueue = topicAndPartitions.map(_ -> new LinkedBlockingQueue[KafkaMessage]()).toMap

  private val fetchThread = new Thread {
    override def run(): Unit = {
      while (!Thread.currentThread.isInterrupted) {
        val msg = fetchMessage()
        if (msg != null) {
          incomingQueue(msg.topicAndPartition).put(msg)
        } else if (noMessages.size == topicAndPartitions.size) {
          LOG.info(s"no messages for all TopicAndPartitions. sleeping for ${noMessageSleepMS} ms")
          Thread.sleep(noMessageSleepMS)
        }
      }
    }
  }

  def start(): Unit = {
    fetchThread.start()
  }

  def setStartOffset(topicAndPartition: TopicAndPartition, startOffset: Long): Unit = {
    iterators(topicAndPartition).setStartOffset(startOffset)
  }

  def nextMessage(topicAndPartition: TopicAndPartition): KafkaMessage = {
    incomingQueue(topicAndPartition).poll()
  }

  // fetch message from each TopicAndPartition in a round-robin way
  private def fetchMessage(): KafkaMessage = {
    val msg = fetchMessage(topicAndPartitions(partitionIndex))
    partitionIndex = (partitionIndex + 1) % partitionNum
    msg
  }

  private def fetchMessage(topicAndPartition: TopicAndPartition): KafkaMessage = {
    val iter = iterators(topicAndPartition)
    if (iter.hasNext) {
      if (noMessages(topicAndPartition)) {
        noMessages -= topicAndPartition
      }
      KafkaMessage(topicAndPartition, iter.getOffset, iter.getKey, iter.next)
    } else {
      noMessages += topicAndPartition
      null
    }
  }

  def close(): Unit = {
    iterators.foreach(_._2.close())
  }
}

class MessageIterator(host: String,
                      port: Int,
                      topic: String,
                      partition: Int,
                      soTimeout: Int,
                      bufferSize: Int,
                      fetchSize: Int,
                      clientId: String) {


  private val consumer = new SimpleConsumer(host, port, soTimeout, bufferSize, clientId)
  private var startOffset = consumer.earliestOrLatestOffset(TopicAndPartition(topic, partition),
    OffsetRequest.EarliestTime, -1)
  private var iter = iterator(startOffset)
  private var readMessages = 0L
  private var offset = startOffset
  private var key: Array[Byte] = null
  private var nextOffset = offset

  def setStartOffset(startOffset: Long): Unit = {
    this.startOffset = startOffset
  }

  def getKey: Array[Byte] = {
    key
  }

  def getOffset: Long = {
    offset
  }

  def next: Array[Byte] = {
    val mo = iter.next()
    val message = mo.message
    readMessages += 1
    offset = mo.offset
    key = Utils.readBytes(message.key)
    nextOffset = mo.nextOffset
    Utils.readBytes(mo.message.payload)
  }


  @annotation.tailrec
  final def hasNext: Boolean = {
    if (iter.hasNext) {
      true
    } else if (0 == readMessages) {
      close()
      false
    } else {
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
