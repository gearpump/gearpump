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

package io.gearpump.streaming.kafka

import java.util.Properties

import io.gearpump.streaming.kafka.lib.{DefaultMessageDecoder, KafkaSourceConfig, KafkaUtil, KafkaOffsetManager}
import io.gearpump.streaming.kafka.lib.consumer.{FetchThread, KafkaMessage}
import io.gearpump.streaming.source.DefaultTimeStampFilter
import io.gearpump.streaming.task.TaskContext
import io.gearpump.streaming.transaction.api._
import kafka.common.TopicAndPartition
import OffsetStorage.StorageEmpty
import io.gearpump.util.LogUtil
import io.gearpump.{Message, TimeStamp}
import org.slf4j.Logger

import scala.util.{Failure, Success}


object KafkaSource {
  private val LOG: Logger = LogUtil.getLogger(classOf[KafkaSource])
}

/**
 * Kafka source connectors that pulls a batch of messages (`kafka.consumer.emit.batch.size`)
 * from multiple Kafka TopicAndPartition in a round-robin way.
 *
 * This is a TimeReplayableSource which is able to replay messages given a start time.
 * Each kafka message is tagged with a timestamp by [[io.gearpump.streaming.transaction.api.MessageDecoder]] and the (offset, timestamp) mapping
 * is stored to a [[OffsetStorage]]. On recovery, we could retrieve the previously stored offset
 * from the [[OffsetStorage]] by timestamp and start to read from there.
 *
 * kafka message is wrapped into gearpump [[Message]] and further filtered by a [[TimeStampFilter]]
 * such that obsolete messages are dropped.
 *
 * @param config kafka source config
 * @param offsetStorageFactory factory to build [[OffsetStorage]]
 * @param messageDecoder decodes [[Message]] from raw bytes
 * @param timestampFilter filters out message based on timestamp
 * @param fetchThread fetches messages and puts on a in-memory queue
 * @param offsetManagers manages offset-to-timestamp storage for each kafka.common.TopicAndPartition
 */
class KafkaSource(
    config: KafkaSourceConfig,
    offsetStorageFactory: OffsetStorageFactory,
    messageDecoder: MessageDecoder = new DefaultMessageDecoder,
    timestampFilter: TimeStampFilter = new DefaultTimeStampFilter,
    private var fetchThread: Option[FetchThread] = None,
    private var offsetManagers: Map[TopicAndPartition, KafkaOffsetManager] = Map.empty[TopicAndPartition, KafkaOffsetManager])
  extends TimeReplayableSource {
  import KafkaSource._

  private var startTime: Option[TimeStamp] = None
  private var currentMessage: Option[Message] = None

  /**
   * @param topics comma-separated string of topics
   * @param properties kafka consumer config
   * @param offsetStorageFactory [[io.gearpump.streaming.transaction.api.OffsetStorageFactory]]
   *                            that creates [[io.gearpump.streaming.transaction.api.OffsetStorage]]
   */
  def this(topics: String, properties: Properties, offsetStorageFactory: OffsetStorageFactory) = {
    this(KafkaSourceConfig(properties).withConsumerTopics(topics), offsetStorageFactory)
  }
  /**
   * @param topics comma-separated string of topics
   * @param properties kafka consumer config
   * @param offsetStorageFactory [[io.gearpump.streaming.transaction.api.OffsetStorageFactory]]
   *                            that creates [[io.gearpump.streaming.transaction.api.OffsetStorage]]
   * @param messageDecoder decodes [[Message]] from raw bytes
   * @param timestampFilter filters out message based on timestamp
   */
  def this(topics: String, properties: Properties, offsetStorageFactory: OffsetStorageFactory,
           messageDecoder: MessageDecoder,
           timestampFilter: TimeStampFilter) = {
    this(KafkaSourceConfig(properties)
      .withConsumerTopics(topics), offsetStorageFactory,
      messageDecoder, timestampFilter)
  }

  /**
   * @param topics comma-separated string of topics
   * @param zkConnect kafka consumer config `zookeeper.connect`
   * @param offsetStorageFactory [[io.gearpump.streaming.transaction.api.OffsetStorageFactory]]
   *                            that creates [[io.gearpump.streaming.transaction.api.OffsetStorage]]
   */
  def this(topics: String, zkConnect: String, offsetStorageFactory: OffsetStorageFactory) =
    this(topics, KafkaUtil.buildConsumerConfig(zkConnect), offsetStorageFactory)

  /**
   * @param topics comma-separated string of topics
   * @param zkConnect kafka consumer config `zookeeper.connect`
   * @param offsetStorageFactory [[io.gearpump.streaming.transaction.api.OffsetStorageFactory]]
   *                            that creates [[io.gearpump.streaming.transaction.api.OffsetStorage]]
   * @param messageDecoder decodes [[Message]] from raw bytes
   * @param timestampFilter filters out message based on timestamp
   */
  def this(topics: String, zkConnect: String, offsetStorageFactory: OffsetStorageFactory,
           messageDecoder: MessageDecoder,
           timestampFilter: TimeStampFilter) = {
    this(topics, KafkaUtil.buildConsumerConfig(zkConnect), offsetStorageFactory,
      messageDecoder, timestampFilter)
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

  override def open(context: TaskContext, startTime: TimeStamp): Unit = {
    import context.{appId, appName, parallelism, taskId}

    val topics = config.getConsumerTopics
    val grouper = config.getGrouper
    val consumerConfig = config.consumerConfig
    val topicAndPartitions = grouper.group(parallelism, taskId.index,
      KafkaUtil.getTopicAndPartitions(KafkaUtil.connectZookeeper(consumerConfig)(), topics))
    this.fetchThread = Some(FetchThread(topicAndPartitions, config.getFetchThreshold,
      config.getFetchSleepMS, config.getConsumerStartOffset, consumerConfig))
    this.offsetManagers = topicAndPartitions.map { tp =>
      val storageTopic = s"app${appId}_${appName}_${tp.topic}_${tp.partition}"
      val storage = offsetStorageFactory.getOffsetStorage(storageTopic)
      tp -> new KafkaOffsetManager(storage)
    }.toMap

    setStartTime(Option(startTime))
  }

  override def advance(): Boolean = {
    currentMessage = fetchThread.flatMap(_.poll.flatMap(filterMessage))
    currentMessage.nonEmpty
  }

  override def read(): Message = {
    currentMessage.getOrElse(throw new NoSuchElementException)
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
