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
import org.apache.gearpump.streaming.kafka.lib._
import org.apache.gearpump.streaming.kafka.lib.consumer.{KafkaMessage, FetchThread}
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.streaming.transaction.api.OffsetStorage.StorageEmpty
import org.apache.gearpump.streaming.transaction.api._
import org.apache.gearpump.util.LogUtil
import org.apache.gearpump.{Message, TimeStamp}
import org.slf4j.Logger

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}


object KafkaSource {
  private val LOG: Logger = LogUtil.getLogger(classOf[KafkaSource])
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
 * @param config
 * @param fetchThread
 * @param offsetManagers
 */
class KafkaSource private[kafka](config: KafkaConfig,
                                 private var fetchThread: Option[FetchThread],
                                 private var offsetManagers: Map[TopicAndPartition, KafkaOffsetManager])
  extends TimeReplayableSource {
  import org.apache.gearpump.streaming.kafka.KafkaSource._

  private lazy val messageDecoder = config.getMessageDecoder
  private lazy val timestampFilter = config.getTimeStampFilter
  private lazy val batchSize = config.getConsumerEmitBatchSize
  private var startTime: Option[TimeStamp] = None

  def this(config: KafkaConfig) = {
    this(config, None, Map.empty[TopicAndPartition, KafkaOffsetManager])
  }


  LOG.debug(s"assigned ${offsetManagers.keySet}")


  private[kafka] def setStartTime(startTime: Option[TimeStamp]): Unit = {
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

  override def open(context: TaskContext, startTime: Option[TimeStamp]): Unit = {
    import context.{appName, parallelism, taskId}

    val grouper = config.getGrouperFactory.getKafkaGrouper(taskId, parallelism)
    val consumerConfig = new ConsumerConfig(config.consumerConfig)
    val topicAndPartitions = KafkaUtil.getTopicAndPartitions(KafkaUtil.connectZookeeper(consumerConfig)(),
      grouper, config.getConsumerTopics)
    this.fetchThread = Some(FetchThread(topicAndPartitions, config.getFetchThreshold,
      config.getFetchSleepMS, consumerConfig))
    this.offsetManagers = topicAndPartitions.map { tp =>
      val storageTopic = s"${appName}_${tp.topic}_${tp.partition}"
      val connectZk = KafkaUtil.connectZookeeper(consumerConfig)
      val topicExists = KafkaUtil.createTopic(connectZk(), storageTopic, partitions = 1, config.getStorageReplicas)
      val storage = KafkaStorage(storageTopic, topicExists, consumerConfig, config.producerConfig)
      tp -> new KafkaOffsetManager(storage)
    }.toMap

    setStartTime(startTime)
  }

  override def read(): List[Message] = {
    val messageBuffer = ArrayBuffer.empty[Message]

    fetchThread.foreach {
      fetch =>
        var count = 0
        while (count < batchSize) {
          fetch.poll.flatMap(filterMessage).foreach(messageBuffer += _)
          count += 1
        }
    }
    messageBuffer.toList
  }

  private def filterMessage(kafkaMsg: KafkaMessage): Option[Message] = {
    val msg: Option[Message] = offsetManagers(kafkaMsg.topicAndPartition)
      .filter(messageDecoder.fromBytes(kafkaMsg.msg) -> kafkaMsg.offset)
    startTime match {
      case Some(t) =>
        msg.flatMap(m => timestampFilter.filter(m, t))
      case None =>
        msg
    }
  }

  override def close(): Unit = {
    offsetManagers.foreach(_._2.close())
  }

}
