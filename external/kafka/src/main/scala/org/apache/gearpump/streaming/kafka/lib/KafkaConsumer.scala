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

import java.util.concurrent.LinkedBlockingQueue

import kafka.common.TopicAndPartition
import org.I0Itec.zkclient.ZkClient
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger


case class KafkaMessage(topicAndPartition: TopicAndPartition, offset: Long,
                        key: Option[Array[Byte]], msg: Array[Byte])

object KafkaConsumer {

  // TODO: use kafka.cluster.Broker once it is not package private in 0.8.2
  object Broker {
    def toString(brokers: List[Broker]) = brokers.mkString(",")
  }

  case class Broker(host: String, port: Int) {
    override def toString = s"$host:$port"
  }

  def apply(topicAndPartitions: Array[TopicAndPartition],
            clientId: String,
            socketTimeout: Int,
            socketBufferSize: Int,
            fetchSize: Int,
            zkClient: ZkClient,
            fetchThreshold: Int,
            fetchSleepMS: Int): KafkaConsumer = {
    val leaders = topicAndPartitions.map {
      tp => {
        tp -> KafkaUtil.getBroker(zkClient, tp.topic, tp.partition)
      }
    }.toMap
    val iterators: Map[TopicAndPartition, KafkaMessageIterator] = leaders.map {
      case (tp, broker) =>
        tp -> KafkaMessageIterator(broker.host, broker.port, tp.topic, tp.partition,
          socketTimeout, socketBufferSize, fetchSize, clientId)
    }
    val incomingQueue = new LinkedBlockingQueue[KafkaMessage]()
    val fetchThread = new FetchThread(topicAndPartitions, iterators,
      incomingQueue, fetchThreshold, fetchSleepMS)
    new KafkaConsumer(fetchThread, incomingQueue)
  }

  private val LOG: Logger = LogUtil.getLogger(getClass)
}


class KafkaConsumer(fetchThread: FetchThread,
                    incomingQueue: LinkedBlockingQueue[KafkaMessage])  {


  def start(): Unit = {
    fetchThread.start()
  }

  def setStartOffset(topicAndPartition: TopicAndPartition, startOffset: Long): Unit = {
    fetchThread.setStartOffset(topicAndPartition, startOffset)
  }

  /**
   * retrieve and remove head message of the incomingQueue,
   * and return None if the queue is empty
   */
  def pollNextMessage: Option[KafkaMessage] = {
    Option(incomingQueue.poll())
  }

  def close(): Unit = {
    fetchThread.interrupt()
    fetchThread.join()
  }
}


