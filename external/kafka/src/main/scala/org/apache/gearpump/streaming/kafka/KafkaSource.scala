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
import kafka.consumer.ConsumerConfig
import org.apache.gearpump.streaming.kafka.lib.consumer.{KafkaConsumerConfig, FetchThread}
import org.apache.gearpump.streaming.kafka.lib.grouper.KafkaGrouper
import org.apache.gearpump.streaming.kafka.lib.producer.KafkaProducerConfig
import org.apache.gearpump.streaming.task.TaskId
import org.apache.gearpump.streaming.kafka.lib._
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
  import org.apache.gearpump.streaming.kafka.KafkaSource._

  private[kafka] def this(appName: String,
                          topicAndPartitions: Array[TopicAndPartition],
                          messageDecoder: MessageDecoder,
                          kafkaConfig: KafkaConfig,
                          consumerConfig: ConsumerConfig,
                          producerConfig: KafkaProducerConfig) =
    this(FetchThread(topicAndPartitions, kafkaConfig, consumerConfig), messageDecoder,
      topicAndPartitions.map(tp =>
        tp -> KafkaOffsetManager(appName, tp, kafkaConfig, consumerConfig, producerConfig)).toMap)

  private[kafka] def this(appName: String,
                          grouper: KafkaGrouper,
                          messageDecoder: MessageDecoder,
                          kafkaConfig: KafkaConfig,
                          consumerConfig: ConsumerConfig,
                          producerConfig: KafkaProducerConfig) =
    this(appName, KafkaUtil.getTopicAndPartitions(KafkaUtil.connectZookeeper(consumerConfig)(),
      grouper, kafkaConfig.getConsumerTopics), messageDecoder, kafkaConfig, consumerConfig, producerConfig)

  private[kafka] def this(appName : String,
           taskId: TaskId,
           taskParallelism: Int,
           messageDecoder: MessageDecoder,
           kafkaConfig: KafkaConfig,
           consumerConfig: KafkaConsumerConfig,
           producerConfig: KafkaProducerConfig) =
    this(appName, kafkaConfig.getGrouperFactory.getKafkaGrouper(taskId, taskParallelism),
      messageDecoder, kafkaConfig, consumerConfig.config, producerConfig)

  def this(appName: String, taskId: TaskId, taskParallelism: Int, kafkaConfig: KafkaConfig, messageDecoder: MessageDecoder) = {
    this(appName, taskId, taskParallelism, messageDecoder, kafkaConfig, kafkaConfig.consumerConfig, kafkaConfig.producerConfig)
  }

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

  override def pull(number: Int): List[Message] = {
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
