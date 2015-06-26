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
import org.apache.gearpump.streaming.task.{TaskId, TaskContext}
import org.apache.gearpump.streaming.transaction.api.OffsetStorage.StorageEmpty
import org.apache.gearpump.streaming.transaction.api._
import org.apache.gearpump.util.LogUtil
import org.apache.gearpump.{Message, TimeStamp}
import org.slf4j.Logger

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}


object KafkaSource {
  private val LOG: Logger = LogUtil.getLogger(classOf[KafkaSource])

  def apply(appName: String,
            taskId: TaskId,
            taskParallelism: Int,
            kafkaConfig: KafkaConfig): KafkaSource = {
    val topicAndPartitions = getTopicAndPartitions(taskId, taskParallelism, kafkaConfig)
    val fetchThread = Some(createFetchThread(topicAndPartitions, kafkaConfig))
    val offsetManagers = createOffsetManagers(appName, topicAndPartitions, kafkaConfig)
    new KafkaSource(kafkaConfig, fetchThread, offsetManagers)
  }

  def getTopicAndPartitions(taskId: TaskId, taskParallelism: Int, kafkaConfig: KafkaConfig): Array[TopicAndPartition] = {
    val grouper = kafkaConfig.getGrouperFactory.getKafkaGrouper(taskId, taskParallelism)
     KafkaUtil.getTopicAndPartitions(KafkaUtil.connectZookeeper(kafkaConfig)(),
      grouper, kafkaConfig.getConsumerTopics)
  }

  def createFetchThread(topicAndPartitions: Array[TopicAndPartition], kafkaConfig: KafkaConfig): FetchThread = {
    FetchThread(topicAndPartitions, kafkaConfig)
  }

  def createOffsetManagers(appName: String,
                           topicAndPartitions: Array[TopicAndPartition],
                           kafkaConfig: KafkaConfig): Map[TopicAndPartition, KafkaOffsetManager] = {
    topicAndPartitions.map(tp => tp -> KafkaOffsetManager(appName, kafkaConfig, tp)).toMap
  }

}

/**
 * Kafka source connectors that pulls a batch of messages (`kafka.consumer.emit.batch.size`)
 * from multiple Kafka [[TopicAndPartition]]s in a round-robin way.
 *
 * This is a TimeReplayableSource which is able to replay messages given a start time.
 * Each kafka message is tagged with a timestamp by [[MessageDecoder]] and the (offset, timestamp) mapping
 * is stored to a [[OffsetStorage]]. On recovery, we could retrieve the previously stored offset
 * from the [[OffsetStorage]] by timestamp and start to read from there.
 *
 * kafka message is wrapped into gearpump [[Message]] and further filtered by a [[TimeStampFilter]]
 * such that obsolete messages are dropped.
 *
 *
 * @param kafkaConfig
 * @param fetchThread
 * @param offsetManagers
 */
class KafkaSource private[kafka](kafkaConfig: KafkaConfig,
                                 private var fetchThread: Option[FetchThread],
                                 private var offsetManagers: Map[TopicAndPartition, KafkaOffsetManager])
  extends TimeReplayableSource {
  import org.apache.gearpump.streaming.kafka.source.KafkaSource._

  private lazy val messageDecoder = kafkaConfig.getMessageDecoder
  private lazy val timestampFilter = kafkaConfig.getTimeStampFilter
  private lazy val batchSize = kafkaConfig.getConsumerEmitBatchSize
  private var startTime: Option[TimeStamp] = None

  def this(kafkaConfig: KafkaConfig) = {
    this(kafkaConfig, None, Map.empty[TopicAndPartition, KafkaOffsetManager])
  }


  LOG.debug(s"assigned ${offsetManagers.keySet}")


  override def setStartTime(startTime: Option[TimeStamp]): Unit = {
    this.startTime = startTime
    fetchThread.foreach { fetch =>
      this.startTime.foreach { time =>
        offsetManagers.foreach { case (tp, offsetManager) =>
          offsetManager.resolveOffset(time) match {
            case Success(offset) =>
              LOG.debug(s"set start offset to $offset for $tp")
              fetch.setStartOffset(tp, offset)
            case Failure(StorageEmpty) =>
              LOG.debug(s"no previous TimeStamp stored")
            case Failure(e) => throw e
          }
        }
      }
      fetch.setDaemon(true)
      fetch.start()
    }
  }

  override def open(context: TaskContext, startTime: TimeStamp): Unit = {
    import context.{appName, parallelism, taskId}

    val topicAndPartitions = getTopicAndPartitions(taskId, parallelism, kafkaConfig)
    this.fetchThread = Some(createFetchThread(topicAndPartitions, kafkaConfig))
    this.offsetManagers = createOffsetManagers(appName, topicAndPartitions, kafkaConfig)

    setStartTime(Some(startTime))
  }

  override def read(): List[Message] = {
    val messageBuffer = ArrayBuffer.empty[Message]

    fetchThread.foreach { fetch =>
      var count = 0
      while (count < batchSize) {
        fetch.poll.foreach { kafkaMsg =>
          val msg = offsetManagers(kafkaMsg.topicAndPartition)
            .filter(messageDecoder.fromBytes(kafkaMsg.msg) -> kafkaMsg.offset)
          msg.foreach { m =>
            startTime match {
              case Some(t) =>
                timestampFilter.filter(m, t).foreach(messageBuffer += _)
              case None =>
                messageBuffer += m
            }
          }
        }
        count += 1
      }
    }
    messageBuffer.toList
  }

  override def close(): Unit = {
    offsetManagers.foreach(_._2.close())
  }

}
