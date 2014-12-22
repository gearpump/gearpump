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

import kafka.common.TopicAndPartition

import org.I0Itec.zkclient.ZkClient
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.transaction.checkpoint.TimeExtractor
import org.apache.gearpump.util.LogUtil
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
    override def toString = s"$host:$port"
  }

  private val LOG: Logger = LogUtil.getLogger(getClass)
}


class KafkaConsumer(topicAndPartitions: Array[TopicAndPartition],
                    clientId: String, socketTimeout: Int,
                    socketBufferSize: Int, fetchSize: Int,
                    zkClient: ZkClient, fetchThreshold: Int,
                    timeExtractor: TimeExtractor[KafkaMessage])  {
  import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaConsumer._

  private val leaders: Map[TopicAndPartition, Broker] = topicAndPartitions.map {
    tp => {
      tp -> KafkaUtil.getBroker(zkClient, tp.topic, tp.partition)
    }
  }.toMap

  private val fetchThreads: Map[Broker, FetchThread] =
    leaders.foldLeft(Map.empty[Broker, FetchThread]){
      (accum, iter) => {
        val tp = iter._1
        val broker = iter._2
        if (!accum.contains(broker)) {
          val fetchThread = new FetchThread(broker.host, broker.port)
          fetchThread.addTopicAndPartition(tp)
          accum + (broker -> fetchThread)
        } else {
          accum.get(broker).get.addTopicAndPartition(tp)
          accum
        }
      }
    }

  private val incomingQueue = topicAndPartitions.map(_ -> new LinkedBlockingQueue[(KafkaMessage, TimeStamp)]()).toMap

  def startAll(): Unit = {
    fetchThreads.foreach(_._2.start())
  }

  def start(topicAndPartition: TopicAndPartition): Unit = {
    fetchThreads(leaders(topicAndPartition)).start()
  }

  def setStartEndOffsets(topicAndPartition: TopicAndPartition, startOffset: Long, endOffset: Option[Long]): Unit = {
    fetchThreads(leaders(topicAndPartition)).setStartEndOffsets(topicAndPartition, startOffset, endOffset)
  }

  def pollNextMessage(topicAndPartition: TopicAndPartition): (KafkaMessage, TimeStamp) = {
    incomingQueue(topicAndPartition).poll()
  }

  def takeNextMessage(topicAndPartition: TopicAndPartition): (KafkaMessage, TimeStamp) = {
    incomingQueue(topicAndPartition).take()
  }

  def close(): Unit = {
    zkClient.close()
    fetchThreads.foreach(_._2.interrupt())
    fetchThreads.foreach(_._2.join())
  }

  class FetchThread(host: String, port: Int) extends Thread {
    private var topicAndPartitions: List[TopicAndPartition] = List.empty[TopicAndPartition]
    private var iterators: Map[TopicAndPartition, MessageIterator] = Map.empty[TopicAndPartition, MessageIterator]

    def addTopicAndPartition(topicAndPartition: TopicAndPartition) = {
      topicAndPartitions :+= topicAndPartition
      val iter = new MessageIterator(host, port, topicAndPartition.topic, topicAndPartition.partition,
      socketTimeout, socketBufferSize, fetchSize, clientId)
      iterators += topicAndPartition -> iter
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

    private def fetchMessage = {
      topicAndPartitions.foreach {
        tp => {
          val queue = incomingQueue(tp)
          if (queue.size < fetchThreshold) {
            val iter = iterators(tp)
            if (iter.hasNext) {
              val (offset, key, payload) = iter.next
              val msg = KafkaMessage(tp, offset, key, payload)
              queue.put((msg, timeExtractor(msg)))
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


