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

import kafka.common.TopicAndPartition
import kafka.utils.ZkUtils
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.streaming.transaction.api.Storage.StorageEmpty
import org.apache.gearpump.streaming.transaction.api._
import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaConfig._
import org.apache.gearpump.streaming.transaction.lib.kafka.grouper.KafkaGrouper
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
    new KafkaSource(grouper, consumer, messageDecoder, offsetManagers)
  }
}

class KafkaSource(grouper: KafkaGrouper,
                  consumer: KafkaConsumer,
                  messageDecoder: MessageDecoder,
                  offsetManagers: Map[TopicAndPartition, KafkaOffsetManager]) extends TimeReplayableSource {
  import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaSource._

  private var startTime: TimeStamp = 0L

  override def setStartTime(startTime: TimeStamp): Unit = {
    this.startTime = startTime
    offsetManagers.foreach { case (tp, offsetManager) =>
      offsetManager.resolveOffset(this.startTime) match {
        case Success(offset) =>
          LOG.info(s"set start offset to $offset for $tp")
          consumer.setStartEndOffsets(tp, offset, None)
        case Failure(StorageEmpty) =>
          LOG.info(s"no previous TimeStamp stored")
        case Failure(e) => throw e
      }
    }
    consumer.start()
  }

  override def pull(num: Int): List[Message] = {
    @annotation.tailrec
    def pullHelper(i: Int, msgList: List[Message]): List[Message] = {
      if (i >= num) {
        msgList
      } else {
        val srcMsg: Option[Message] = consumer.pollNextMessage.flatMap { kafkaMsg =>
          val msg = messageDecoder.fromBytes(kafkaMsg.msg)
          offsetManagers(kafkaMsg.topicAndPartition).filter(msg -> kafkaMsg.offset)
        }
        if (srcMsg.isDefined) {
          pullHelper(i + 1, msgList :+ srcMsg.get)
        } else {
          pullHelper(i, msgList)
        }
      }
    }
    pullHelper(0, List.empty[Message])
  }
}
