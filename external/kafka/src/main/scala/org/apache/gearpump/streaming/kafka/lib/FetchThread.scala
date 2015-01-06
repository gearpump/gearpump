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

import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.LinkedBlockingQueue

import kafka.common.TopicAndPartition
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

object FetchThread {
  private val LOG: Logger = LogUtil.getLogger(classOf[FetchThread])
}

class FetchThread(topicAndPartitions: Array[TopicAndPartition],
                  iterators: Map[TopicAndPartition, KafkaMessageIterator],
                  incomingQueue: LinkedBlockingQueue[KafkaMessage],
                  fetchThreshold: Int,
                  fetchSleepMS: Int) extends Thread {
  import org.apache.gearpump.streaming.kafka.lib.FetchThread._


  def setStartOffset(tp: TopicAndPartition, startOffset: Long): Unit = {
    iterators(tp).setStartOffset(startOffset)
  }

  override def run(): Unit = {
    try {
      while (!Thread.currentThread.isInterrupted
        && fetchMessage) {
        if (incomingQueue.size >= fetchThreshold) {
          Thread.sleep(fetchSleepMS)
        }
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
  private[kafka] def fetchMessage: Boolean = {
    topicAndPartitions.foldLeft(false) { (hasNext, tp) => {
        if (incomingQueue.size < fetchThreshold) {
          val iter = iterators(tp)
          if (iter.hasNext) {
            incomingQueue.put(iter.next())
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
}
