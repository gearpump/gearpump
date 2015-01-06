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

package org.apache.gearpump.streaming.kafka

import kafka.common.TopicAndPartition
import kafka.utils.ZkUtils
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.streaming.kafka.lib.{KafkaOffsetManager, KafkaConsumer}
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig._
import org.apache.gearpump.streaming.transaction.api.OffsetStorage.StorageEmpty
import org.apache.gearpump.streaming.transaction.api._
import org.apache.gearpump.util.LogUtil
import org.apache.gearpump.{Message, TimeStamp}
import org.slf4j.Logger

import scala.util.{Failure, Success}


object KafkaSource {
  private val LOG: Logger = LogUtil.getLogger(classOf[KafkaSource])

  def apply(appId : Int, taskContext : TaskContext, conf: UserConfig, messageDecoder: MessageDecoder): KafkaSource = {
    val config = conf.config
    val grouper = config.getGrouperFactory.getKafkaGrouper(taskContext)
    val topicAndPartitions = grouper.group(
      ZkUtils.getPartitionsForTopics(config.getZkClient(), config.getConsumerTopics)
      .flatMap { case (topic, partitions) =>
        partitions.map(TopicAndPartition(topic, _)) }.toArray)
    val consumer: KafkaConsumer = config.getConsumer(topicAndPartitions = topicAndPartitions)
    val offsetManagers: Map[TopicAndPartition, KafkaOffsetManager] =
      topicAndPartitions.map(tp => tp -> KafkaOffsetManager(appId, conf, tp)).toMap
    new KafkaSource(consumer, messageDecoder, offsetManagers)
  }
}

class KafkaSource(consumer: KafkaConsumer,
                  messageDecoder: MessageDecoder,
                  offsetManagers: Map[TopicAndPartition, KafkaOffsetManager]) extends TimeReplayableSource {
  import org.apache.gearpump.streaming.kafka.KafkaSource._

  private var startTime: TimeStamp = 0L

  override def setStartTime(startTime: TimeStamp): Unit = {
    this.startTime = startTime
    offsetManagers.foreach { case (tp, offsetManager) =>
      offsetManager.resolveOffset(this.startTime) match {
        case Success(offset) =>
          LOG.debug(s"set start offset to $offset for $tp")
          consumer.setStartOffset(tp, offset)
        case Failure(StorageEmpty) =>
          LOG.debug(s"no previous TimeStamp stored")
        case Failure(e) => throw e
      }
    }
    consumer.start()
  }

  override def pull(number: Int): List[Message] = {
    @annotation.tailrec
    def pullHelper(count: Int, msgList: List[Message]): List[Message] = {
      if (count >= number) {
        msgList
      } else {
        val optMsg: Option[Message] = consumer.pollNextMessage.flatMap { kafkaMsg =>
          val msg = messageDecoder.fromBytes(kafkaMsg.msg)
          offsetManagers(kafkaMsg.topicAndPartition).filter(msg -> kafkaMsg.offset)
        }
        optMsg match {
          case Some(msg) => pullHelper(count + 1, msgList :+ msg)
          case None      => pullHelper(count + 1, msgList)
        }
      }
    }
    pullHelper(0, List.empty[Message])
  }
}
