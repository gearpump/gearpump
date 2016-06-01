/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming.kafka.lib.source

import java.util.Properties

import com.twitter.bijection.Injection
import kafka.common.TopicAndPartition
import org.apache.gearpump.streaming.kafka.KafkaSource
import org.apache.gearpump.streaming.kafka.lib.source.consumer.FetchThread.FetchThreadFactory
import org.apache.gearpump.streaming.kafka.lib.util.KafkaClient
import KafkaClient.KafkaClientFactory
import org.apache.gearpump.streaming.kafka.lib.source.consumer.{KafkaMessage, FetchThread}
import org.apache.gearpump.streaming.kafka.lib.source.grouper.PartitionGrouper
import org.apache.gearpump.streaming.kafka.lib.util.KafkaClient
import org.apache.gearpump.streaming.kafka.util.KafkaConfig
import org.apache.gearpump.streaming.kafka.util.KafkaConfig.KafkaConfigFactory
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.streaming.transaction.api._
import org.apache.gearpump.util.LogUtil
import org.apache.gearpump.{Message, TimeStamp}
import org.slf4j.Logger

object AbstractKafkaSource {
  private val LOG: Logger = LogUtil.getLogger(classOf[KafkaSource])
}

/**
 * Contains implementation for Kafka source connectors, users should use
 * [[org.apache.gearpump.streaming.kafka.KafkaSource]].
 *
 * This is a TimeReplayableSource which is able to replay messages given a start time.
 * Each kafka message is tagged with a timestamp by
 * [[org.apache.gearpump.streaming.transaction.api.MessageDecoder]] and the (timestamp, offset)
 * mapping is stored to a [[org.apache.gearpump.streaming.transaction.api.CheckpointStore]].
 * On recovery, we could retrieve the previously stored offset from the
 * [[org.apache.gearpump.streaming.transaction.api.CheckpointStore]] by timestamp and start to read
 * from there.
 *
 * kafka message is wrapped into gearpump [[org.apache.gearpump.Message]] and further filtered by a
 * [[org.apache.gearpump.streaming.transaction.api.TimeStampFilter]]
 * such that obsolete messages are dropped.
 */
abstract class AbstractKafkaSource(
    topic: String,
    props: Properties,
    kafkaConfigFactory: KafkaConfigFactory,
    kafkaClientFactory: KafkaClientFactory,
    fetchThreadFactory: FetchThreadFactory)
  extends TimeReplayableSource {
  import org.apache.gearpump.streaming.kafka.lib.source.AbstractKafkaSource._

  def this(topic: String, properties: Properties) = {
    this(topic, properties, new KafkaConfigFactory, KafkaClient.factory, FetchThread.factory)
  }

  private lazy val config: KafkaConfig = kafkaConfigFactory.getKafkaConfig(props)
  private lazy val kafkaClient: KafkaClient = kafkaClientFactory.getKafkaClient(config)
  private lazy val fetchThread: FetchThread = fetchThreadFactory.getFetchThread(config, kafkaClient)
  private lazy val messageDecoder = config.getConfiguredInstance(
    KafkaConfig.MESSAGE_DECODER_CLASS_CONFIG, classOf[MessageDecoder])
  private lazy val timestampFilter = config.getConfiguredInstance(
    KafkaConfig.TIMESTAMP_FILTER_CLASS_CONFIG, classOf[TimeStampFilter])

  private var startTime: Long = 0L
  private var checkpointStoreFactory: Option[CheckpointStoreFactory] = None
  private var checkpointStores: Map[TopicAndPartition, CheckpointStore] =
    Map.empty[TopicAndPartition, CheckpointStore]

  override def setCheckpointStore(checkpointStoreFactory: CheckpointStoreFactory): Unit = {
    this.checkpointStoreFactory = Some(checkpointStoreFactory)
  }

  override def open(context: TaskContext, startTime: TimeStamp): Unit = {
    import context.{parallelism, taskId}

    LOG.info("KafkaSource opened at start time {}", startTime)
    this.startTime = startTime
    val topicList = topic.split(",", -1).toList
    val grouper = config.getConfiguredInstance(KafkaConfig.PARTITION_GROUPER_CLASS_CONFIG,
      classOf[PartitionGrouper])
    val topicAndPartitions = grouper.group(parallelism, taskId.index,
      kafkaClient.getTopicAndPartitions(topicList))
    LOG.info("assigned partitions {}", s"Array(${topicAndPartitions.mkString(",")})")

    fetchThread.setTopicAndPartitions(topicAndPartitions)
    maybeSetupCheckpointStores(topicAndPartitions)
    maybeRecover()
  }

  /**
   * Reads a record from incoming queue, decodes, filters and checkpoints offsets
   * before returns a Message. Message can be null if the incoming queue is empty.
   * @return a [[org.apache.gearpump.Message]] or null
   */
  override def read(): Message = {
    fetchThread.poll.flatMap(filterAndCheckpointMessage).orNull
  }

  override def close(): Unit = {
    kafkaClient.close()
    checkpointStores.foreach(_._2.close())
    LOG.info("KafkaSource closed")
  }

  /**
   * 1. Decodes raw bytes into Message with timestamp
   * 2. Filters message against start time
   * 3. Checkpoints (timestamp, kafka_offset)
   */
  private def filterAndCheckpointMessage(kafkaMsg: KafkaMessage): Option[Message] = {
    val msg = messageDecoder.fromBytes(kafkaMsg.key.orNull, kafkaMsg.msg)
    LOG.debug("read message {}", msg)
    val filtered = timestampFilter.filter(msg, startTime)
    filtered.foreach { m =>
      val time = m.timestamp
      val offset = kafkaMsg.offset
      LOG.debug("checkpoint message state ({}, {})", time, offset)
      checkpointOffsets(kafkaMsg.topicAndPartition, time, offset)
    }
    filtered
  }

  private def checkpointOffsets(tp: TopicAndPartition, time: TimeStamp, offset: Long): Unit = {
    checkpointStores.get(tp).foreach(_.persist(time, Injection[Long, Array[Byte]](offset)))
  }

  private def maybeSetupCheckpointStores(tps: Array[TopicAndPartition]): Unit = {
    for {
      f <- checkpointStoreFactory
      tp <- tps
    } {
      val store = f.getCheckpointStore(KafkaConfig.getCheckpointStoreNameSuffix(tp))
      LOG.info("created checkpoint store for {}", tp)
      checkpointStores += tp -> store
    }
  }

  private def maybeRecover(): Unit = {
    checkpointStores.foreach { case (tp, store) =>
      for {
        bytes <- store.recover(startTime)
        offset <- Injection.invert[Long, Array[Byte]](bytes).toOption
      } {
        LOG.info("recovered offset {} for {}", offset, tp)
        fetchThread.setStartOffset(tp, offset)
      }
    }
    // let JVM exit when other threads are closed
    fetchThread.setDaemon(true)
    fetchThread.start()
  }

  protected def addCheckpointStore(tp: TopicAndPartition, store: CheckpointStore): Unit = {
    checkpointStores += tp -> store
  }
}
