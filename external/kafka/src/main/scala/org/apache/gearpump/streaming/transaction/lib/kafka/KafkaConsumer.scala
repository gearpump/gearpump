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

import java.nio.channels.ClosedByInterruptException
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

  private val LOG: Logger = LogUtil.getLogger(getClass)
}


class KafkaConsumer(topicAndPartitions: Array[TopicAndPartition],
                    clientId: String, socketTimeout: Int,
                    socketBufferSize: Int, fetchSize: Int,
                    zkClient: ZkClient, fetchThreshold: Int)  {
  import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaConsumer._

  private val fetchThread: FetchThread = new FetchThread(topicAndPartitions)
  private val incomingQueue = new LinkedBlockingQueue[KafkaMessage]()

  def start(): Unit = {
    fetchThread.start()
  }

  def setStartEndOffsets(topicAndPartition: TopicAndPartition, startOffset: Long, endOffset: Option[Long]): Unit = {
    fetchThread.setStartEndOffsets(topicAndPartition, startOffset, endOffset)
  }

  /**
   * retrieve and remove head message of the incomingQueue,
   * and return None if the queue is empty
   */
  def pollNextMessage: Option[KafkaMessage] = {
    Option(incomingQueue.poll())
  }

  /**
   * retrieve and remove head message of the TopicAndPartition queue,
   * and wait if necessary for an message to become available
   */
  def takeNextMessage: KafkaMessage = {
    incomingQueue.take()
  }

  def close(): Unit = {
    zkClient.close()
    fetchThread.interrupt()
    fetchThread.join()
  }

  class FetchThread(topicAndPartitions: Array[TopicAndPartition]) extends Thread {
    private val leaders: Map[TopicAndPartition, Broker] = topicAndPartitions.map {
      tp => {
        tp -> KafkaUtil.getBroker(zkClient, tp.topic, tp.partition)
      }
    }.toMap

    private val iterators: Map[TopicAndPartition, MessageIterator] = leaders.map {
      case (tp, broker) =>
        tp -> new MessageIterator(broker.host, broker.port, tp.topic, tp.partition,
          socketTimeout, socketBufferSize, fetchSize, clientId)
    }

    def setStartEndOffsets(topicAndPartition: TopicAndPartition, startOffset: Long, endOffset: Option[Long]): Unit = {
      iterators(topicAndPartition).setStartEndOffsets(startOffset, endOffset)
    }

    override def run(): Unit = {
      try {
        while (!Thread.currentThread.isInterrupted
        && !hasNext) {
          fetchMessage
        }
      } catch {
        case e: InterruptedException => LOG.info("fetch thread got interrupted exception")
        case e: ClosedByInterruptException => LOG.info("fetch thread closed by interrupt exception")
      } finally {
        iterators.values.foreach(_.close())
      }
    }

    /**
     * fetch message from each TopicAndPartition in a round-robin way
     */
    private def fetchMessage = {
      topicAndPartitions.foreach {
        tp => {
          if (incomingQueue.size < fetchThreshold) {
            val iter = iterators(tp)
            if (iter.hasNext) {
              val (offset, key, payload) = iter.next
              val msg = KafkaMessage(tp, offset, key, payload)
              incomingQueue.put(msg)
            }
          }
        }
      }
    }

    private def hasNext: Boolean = {
      iterators.values.forall(_.hasNext == false)
    }
  }
}


