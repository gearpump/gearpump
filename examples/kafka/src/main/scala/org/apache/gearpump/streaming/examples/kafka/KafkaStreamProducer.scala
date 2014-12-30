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

import akka.actor.actorRef2Scala
import kafka.common.TopicAndPartition
import kafka.serializer.{Decoder, StringDecoder}
import kafka.utils.ZkUtils
import org.apache.gearpump.{Message, TimeStamp}
import org.apache.gearpump.streaming.task.{TaskActor, TaskContext}
import org.apache.gearpump.streaming.transaction.checkpoint.{TimeStampFilter, OffsetManager}
import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaConfig.ConfigToKafka
import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaSource
import org.apache.gearpump.util.Configs
import org.slf4j.{Logger, LoggerFactory}

/**
 * connect gearpump with kafka
 */
class KafkaStreamProducer(conf: Configs) extends TaskActor(conf) {

  private val config = conf.config
  private val grouper = config.getGrouperFactory.getKafkaGrouper(conf, context)
  private val emitBatchSize = config.getConsumerEmitBatchSize

  private val topicAndPartitions: Array[TopicAndPartition] = {
    val original = ZkUtils.getPartitionsForTopics(config.getZkClient(), config.getConsumerTopics)
      .flatMap(tps => { tps._2.map(TopicAndPartition(tps._1, _)) }).toArray
    val grouped = grouper.group(original)
    grouped.foreach(tp =>
      LOG.info(s"KafkaStreamProducer $taskId has been assigned partition (${tp.topic}, ${tp.partition})"))
    grouped
  }

  private val consumer = config.getConsumer(topicAndPartitions = topicAndPartitions)
  private val decoder: Decoder[String] = new StringDecoder()
  private val offsetManager = new OffsetManager(
    config.getCheckpointManagerFactory.getCheckpointManager[TimeStamp, Long](conf))
  private val msgFilter = new TimeStampFilter(config.getCheckpointMessageDelayMS)
  private val commitIntervalMS = config.getCheckpointCommitIntervalMS
  private var lastCommitTime = System.currentTimeMillis()

  override def onStart(taskContext: TaskContext): Unit = {
    LOG.info(s"start time ${taskContext.startTime}")
    offsetManager.register(topicAndPartitions.map(KafkaSource(_)))
    offsetManager.start()
    val replayConsumer = config.getConsumer(topicAndPartitions = topicAndPartitions)
    offsetManager.replay[String](replayConsumer, taskContext.startTime, decoder, msgFilter, output).foreach {
      case(source, offset) =>
        consumer.setStartEndOffsets(TopicAndPartition(source.name, source.partition), offset, None)
    }
    replayConsumer.close()
    consumer.startAll()

    self ! Message("start", System.currentTimeMillis())
  }

  override def onNext(msg: Message): Unit = {

    @annotation.tailrec
    def fetchAndEmit(msgNum: Int, tpIndex: Int): Unit = {
      if (msgNum < emitBatchSize) {
        val msgWithTime = consumer.pollNextMessage(topicAndPartitions(tpIndex))
        if (msgWithTime != null) {
          val kafkaMsg = msgWithTime._1
          val timestamp =  msgWithTime._2
          output(Message(decoder.fromBytes(kafkaMsg.msg), timestamp))
          offsetManager.update(KafkaSource(kafkaMsg.topicAndPartition), timestamp, kafkaMsg.offset)
          if (shouldCommitCheckpoint) {
            offsetManager.checkpoint
            lastCommitTime = System.currentTimeMillis()
            LOG.info(s"committing checkpoint at $lastCommitTime with offset ${kafkaMsg.offset}")
          }
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
    self ! Message("continue", System.currentTimeMillis())
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
