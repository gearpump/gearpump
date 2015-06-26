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

import java.util.Properties

import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import org.apache.gearpump.streaming.kafka.lib._
import org.apache.gearpump.streaming.kafka.lib.consumer.{FetchThread, KafkaMessage}
import org.apache.gearpump.streaming.source.DefaultTimeStampFilter
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
 * @param config utility class for kafka config, see [[KafkaConfig]]
 * @param messageDecoder decodes [[Message]] from raw bytes
 * @param timestampFilter filters out message based on timestamp
 * @param fetchThread fetches messages and puts on a in-memory queue
 * @param offsetManagers manages offset-to-timestamp storage for each [[TopicAndPartition]]
 */
class KafkaSource (config: KafkaConfig,
                   messageDecoder: MessageDecoder = new DefaultMessageDecoder,
                   timestampFilter: TimeStampFilter = new DefaultTimeStampFilter,
                   private var fetchThread: Option[FetchThread] = None,
                   private var offsetManagers: Map[TopicAndPartition, KafkaOffsetManager] = Map.empty[TopicAndPartition, KafkaOffsetManager])

  extends TimeReplayableSource {
  import org.apache.gearpump.streaming.kafka.KafkaSource._

  private var startTime: Option[TimeStamp] = None

  /**
   * @param topics comma-separated string of topics
   * @param properties kafka consumer config
   */
  def this(topics: String, properties: Properties) = {
    this(KafkaConfig.empty()
      .withConsumerTopics(topics).withConsumerConfig(properties))
  }
  /**
   * @param topics comma-separated string of topics
   * @param properties kafka consumer config
   * @param messageDecoder decodes [[Message]] from raw bytes
   * @param timestampFilter filters out message based on timestamp
   */
  def this(topics: String, properties: Properties,
           messageDecoder: MessageDecoder,
           timestampFilter: TimeStampFilter) = {
    this(KafkaConfig.empty()
      .withConsumerTopics(topics).withConsumerConfig(properties), messageDecoder, timestampFilter)
  }

  /**
   * @param topics comma-separated string of topics
   * @param zkConnect kafka consumer config `zookeeper.connect`
   */
  def this(topics: String, zkConnect: String) =
    this(KafkaUtil.setZookeeperConnect(KafkaConfig.empty()
        .withConsumerTopics(topics), zkConnect))
  /**
   * @param topics comma-separated string of topics
   * @param zkConnect kafka consumer config `zookeeper.connect`
   * @param messageDecoder decodes [[Message]] from raw bytes
   * @param timestampFilter filters out message based on timestamp
   */
  def this(topics: String, zkConnect: String,
           messageDecoder: MessageDecoder,
           timestampFilter: TimeStampFilter) = {
    this(KafkaUtil.setZookeeperConnect(KafkaConfig.empty()
        .withConsumerTopics(topics), zkConnect), messageDecoder, timestampFilter)
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
    import context.{appId, appName, parallelism, taskId}

    val grouper = config.getGrouperFactory.getKafkaGrouper(taskId, parallelism)
    val consumerConfig = new ConsumerConfig(config.consumerConfig)
    val topicAndPartitions = KafkaUtil.getTopicAndPartitions(KafkaUtil.connectZookeeper(consumerConfig)(),
      grouper, config.getConsumerTopics)
    this.fetchThread = Some(FetchThread(topicAndPartitions, config.getFetchThreshold,
      config.getFetchSleepMS, consumerConfig))
    this.offsetManagers = topicAndPartitions.map { tp =>
      val storageTopic = s"app${appId}_${appName}_${tp.topic}_${tp.partition}"
      val connectZk = KafkaUtil.connectZookeeper(consumerConfig)
      val topicExists = KafkaUtil.createTopic(connectZk(), storageTopic, partitions = 1, config.getStorageReplicas)
      val storage = KafkaStorage(storageTopic, topicExists, consumerConfig, config.producerConfig)
      tp -> new KafkaOffsetManager(storage)
    }.toMap

    setStartTime(startTime)
  }

  override def read(batchSize: Int): List[Message] = {
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
    val msgOpt = offsetManagers(kafkaMsg.topicAndPartition)
      .filter(messageDecoder.fromBytes(kafkaMsg.msg) -> kafkaMsg.offset)
    msgOpt.flatMap { msg =>
      startTime match {
        case None =>
          Some(msg)
        case Some(time) =>
          timestampFilter.filter(msg, time)
      }
    }
  }

  override def close(): Unit = {
    offsetManagers.foreach(_._2.close())
  }

}
