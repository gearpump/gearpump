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

package org.apache.gearpump.streaming.kafka.source

import kafka.common.TopicAndPartition
import org.apache.gearpump.streaming.kafka.lib._
import org.apache.gearpump.streaming.kafka.lib.grouper.KafkaGrouper
import org.apache.gearpump.streaming.task.TaskId
import org.apache.gearpump.streaming.transaction.api.OffsetStorage.StorageEmpty
import org.apache.gearpump.streaming.transaction.api._
import org.apache.gearpump.util.LogUtil
import org.apache.gearpump.{Message, TimeStamp}
import org.slf4j.Logger

import scala.collection.mutable
import scala.util.{Failure, Success}


object KafkaSource {
  private val LOG: Logger = LogUtil.getLogger(classOf[KafkaSource])
}

class KafkaSource private[kafka](fetchThread: FetchThread,
                                 messageDecoder: MessageDecoder,
                                 offsetManagers: Map[TopicAndPartition, KafkaOffsetManager]) extends TimeReplayableSource {
  import org.apache.gearpump.streaming.kafka.source.KafkaSource._

  private[kafka] def this(appName: String, config: KafkaConfig, topicAndPartitions: Array[TopicAndPartition], messageDecoder: MessageDecoder) =
    this(FetchThread(topicAndPartitions, config), messageDecoder,
      topicAndPartitions.map(tp => tp -> KafkaOffsetManager(appName, config, tp)).toMap)

  private[kafka] def this(appName: String, config: KafkaConfig, grouper: KafkaGrouper, messageDecoder: MessageDecoder) =
    this(appName, config, KafkaUtil.getTopicAndPartitions(KafkaUtil.connectZookeeper(config)(),
      grouper, config.getConsumerTopics), messageDecoder)

  def this(appName : String, taskId: TaskId, taskParallelism:Int, config: KafkaConfig, messageDecoder: MessageDecoder) =
    this(appName: String, config, config.getGrouperFactory.getKafkaGrouper(taskId, taskParallelism), messageDecoder)

  private var startTime: TimeStamp = 0L

  LOG.debug(s"assigned ${offsetManagers.keySet}")

  /**
   * fetchThread will start from beginning (the earliest offset)
   * on start offset not set
   */
  def startFromBeginning(): Unit = {
    fetchThread.setDaemon(true)
    fetchThread.start()
  }

  override def setStartTime(startTime: TimeStamp): Unit = {
    this.startTime = startTime
    offsetManagers.foreach { case (tp, offsetManager) =>
      offsetManager.resolveOffset(this.startTime) match {
        case Success(offset) =>
          LOG.debug(s"set start offset to $offset for $tp")
          fetchThread.setStartOffset(tp, offset)
        case Failure(StorageEmpty) =>
          LOG.debug(s"no previous TimeStamp stored")
        case Failure(e) => throw e
      }
    }
    fetchThread.setDaemon(true)
    fetchThread.start()
  }

  override def read(number: Int): List[Message] = {
    var count = 0
    val messagesBuilder = new mutable.ArrayBuilder.ofRef[Message]
    while (count < number) {
      fetchThread.poll.flatMap { kafkaMsg =>
        val msg = messageDecoder.fromBytes(kafkaMsg.msg)
        offsetManagers(kafkaMsg.topicAndPartition).filter(msg -> kafkaMsg.offset)
      } map (messagesBuilder += _)
      count += 1
    }
    messagesBuilder.result().toList
  }

  override def close(): Unit = {
    offsetManagers.foreach(_._2.close())
  }

}
