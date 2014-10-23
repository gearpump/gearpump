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

package org.apache.gearpump.streaming.examples.kafka

import kafka.api.{FetchRequestBuilder, TopicMetadataRequest}
import kafka.common.ErrorMapping._
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndOffset
import kafka.serializer.StringDecoder
import kafka.utils.{Utils, ZkUtils}

import org.apache.gearpump.streaming.ConfigsHelper._
import org.apache.gearpump.streaming.transaction.api.{RelaxedTimeFilter, OffsetManager}
import org.apache.gearpump.streaming.transaction.kafka.KafkaConfig._
import org.apache.gearpump.streaming.transaction.kafka.KafkaUtil._
import org.apache.gearpump.streaming.transaction.kafka.{KafkaMessage, KafkaSource, KafkaUtil}
import org.apache.gearpump.{TimeStamp, Message}
import org.apache.gearpump.streaming.task.{TaskContext, TaskActor}
import org.apache.gearpump.util.Configs
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._


object KafkaSpout {

  object Broker {
    def toString(brokers: List[Broker]) = brokers.mkString(",")
  }

  case class Broker(host: String, port: Int) {
    override def toString = s"${host}:${port}"
  }

  private val LOG: Logger = LoggerFactory.getLogger(classOf[KafkaSpout])
}


/**
 * connect gearpump with Kafka
 */
class KafkaSpout(conf: Configs) extends TaskActor(conf) {

  import org.apache.gearpump.streaming.examples.kafka.KafkaSpout._

  private val config = conf.config
  private val grouper = new KafkaDefaultGrouper
  private val emitBatchSize = config.getConsumerEmitBatchSize

  private val topicAndPartitions: Array[TopicAndPartition] = {
    val original = ZkUtils.getPartitionsForTopics(config.getZkClient(), config.getConsumerTopics)
      .flatMap(tps => { tps._2.map(TopicAndPartition(tps._1, _)) }).toArray
    val grouped = grouper.group(original,
      conf.dag.tasks(taskId.groupId).parallism, taskId)
    grouped.foreach(tp =>
      LOG.info(s"spout $taskId has been assigned partition (${tp.topic}, ${tp.partition})"))
    grouped
  }

  private val consumer = config.getConsumer(topicAndPartitions = topicAndPartitions)
  private val decoder = new StringDecoder()
  private val offsetManager = new OffsetManager(
    config.getCheckpointManagerFactory.getCheckpointManager[TimeStamp, Long](conf),
    new RelaxedTimeFilter(config.getCheckpointMessageDelayMS))
  private val commitIntervalMS = config.getCheckpointCommitIntervalMS
  private var lastCommitTime = System.currentTimeMillis()

  override def onStart(taskContext: TaskContext): Unit = {
    offsetManager.register(topicAndPartitions.map(KafkaSource(_)))
    offsetManager.start()
    offsetManager.loadStartingOffsets(taskContext.startTime).foreach{
      entry =>
        val source = entry._1
        val offset = entry._2
        val topicAndPartition = TopicAndPartition(source.name, source.partition)
        consumer.setStartOffset(topicAndPartition, offset)
    }
    consumer.start()
    self ! Message("start", System.currentTimeMillis())
  }

  override def onNext(msg: Message): Unit = {

    @annotation.tailrec
    def fetchAndEmit(msgNum: Int, tpIndex: Int): Unit = {
      if (msgNum < emitBatchSize) {
        val kafkaMsg = consumer.nextMessage(topicAndPartitions(tpIndex))
        if (kafkaMsg != null) {
          val timestamp = System.currentTimeMillis()
          output(new Message(decoder.fromBytes(kafkaMsg.msg)))
          offsetManager.update(KafkaSource(kafkaMsg.topicAndPartition), timestamp, kafkaMsg.offset)
        } else {
          LOG.debug(s"no more messages from ${topicAndPartitions(tpIndex)}")
        }
        // poll message from each TopicAndPartition in a round-robin way
        // TODO: make it configurable
        if (tpIndex + 1 == topicAndPartitions.size) {
          fetchAndEmit(msgNum + 1, 0)
        } else {
          fetchAndEmit(msgNum + 1, tpIndex + 1)
        }
      }
    }
    fetchAndEmit(0, 0)
    if (shouldCommitCheckpoint) {
      LOG.info("committing checkpoint...")
      offsetManager.checkpoint
      lastCommitTime = System.currentTimeMillis()
    }
    self ! Message("continue", Message.noTimeStamp)
  }

  override def onStop(): Unit = {
    consumer.close()
    offsetManager.close()
  }

  private def shouldCommitCheckpoint: Boolean = {
    val now = System.currentTimeMillis()
    (now - lastCommitTime) > commitIntervalMS
  }
}
