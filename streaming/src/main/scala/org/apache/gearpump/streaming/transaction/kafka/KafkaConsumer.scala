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

import kafka.common.TopicAndPartition

import org.I0Itec.zkclient.ZkClient
import org.slf4j.{Logger, LoggerFactory}
import java.util.concurrent.LinkedBlockingQueue


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
                    zkClient: ZkClient, queueSize: Int)  {
  import org.apache.gearpump.streaming.transaction.kafka.KafkaConsumer._

  private val leaders: Map[TopicAndPartition, Broker] = topicAndPartitions.map {
    tp => {
      tp -> KafkaUtil.getBroker(zkClient, tp.topic, tp.partition)
    }
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
  private val incomingQueue = topicAndPartitions.map(_ -> new LinkedBlockingQueue[KafkaMessage](queueSize)).toMap

  private val fetchThread = new Thread {
    override def run(): Unit = {
      while (!Thread.currentThread.isInterrupted) {
        val msg = fetchMessage()
        if (msg != null) {
          incomingQueue(msg.topicAndPartition).put(msg)
        } else if (noMessages.size == topicAndPartitions.size) {
          LOG.debug(s"no messages for all TopicAndPartitions. sleeping for ${noMessageSleepMS} ms")
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
  // TODO: make each MessageIterator run in its own thread
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
    fetchThread.interrupt()
    fetchThread.join()
  }
}


