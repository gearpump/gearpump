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

package org.apache.gearpump.streaming.transaction.checkpoint

import java.io.Serializable

import _root_.kafka.common.TopicAndPartition
import _root_.kafka.serializer.Decoder
import org.apache.gearpump.{Message, TimeStamp}
import org.apache.gearpump.streaming.transaction.checkpoint.api.{Checkpoint, Source, CheckpointManager, CheckpointSerDe}
import com.twitter.bijection._
import org.apache.gearpump.util.LogUtil
import org.apache.gearpump.streaming.transaction.lib.kafka.KafkaConsumer
import org.slf4j.{LoggerFactory, Logger}

import scala.util.{Failure, Success}

object OffsetManager {
  object OffsetSerDe extends CheckpointSerDe[TimeStamp, Long] {
    override def toKeyBytes(timestamp: TimeStamp): Array[Byte] = Injection[Long, Array[Byte]](timestamp)

    override def toValueBytes(offset: Long): Array[Byte] = Injection[Long, Array[Byte]](offset)

    override def fromKeyBytes(bytes: Array[Byte]): TimeStamp = Injection.invert[Long, Array[Byte]](bytes) match {
      case Success(t) => t
      case Failure(e) => throw e
    }

    override def fromValueBytes(bytes: Array[Byte]): Long = Injection.invert[Long, Array[Byte]](bytes) match {
      case Success(l) => l
      case Failure(e) => throw e
    }
  }

  private val LOG: Logger = LogUtil.getLogger(getClass)
}

class OffsetManager(checkpointManager: CheckpointManager[TimeStamp, Long]) {
  import org.apache.gearpump.streaming.transaction.checkpoint.OffsetManager._

  private var offsetsByTimeAndSource = Map.empty[(Source, TimeStamp), Long]

  def start(): Unit = {
    LOG.info("starting offsetManager...")
    checkpointManager.start()
  }

  def register(sources: Array[Source]): Unit = {
    checkpointManager.register(sources)
  }

  /**
   *  we only record the smallest offset at a timestamp for a source
   *  @return whether the new offset is written
   */
  def update(source: Source, timestamp: TimeStamp, offset: Long): Boolean = {
    if (!offsetsByTimeAndSource.contains((source, timestamp)) ||
      offsetsByTimeAndSource.get((source, timestamp)).get > offset) {
      offsetsByTimeAndSource += (source, timestamp) -> offset
      true
    } else {
      false
    }
  }

  def checkpoint: Map[Source, Checkpoint[TimeStamp, Long]] = {
    val checkpointsBySource = offsetsByTimeAndSource
      .groupBy(_._1._1)
      .map { grouped => {
        val source = grouped._1
        // TODO: this is not efficient
        val records = grouped._2
          .map(entry => entry._1._2 -> entry._2)
          .toList.sortBy(_._1)
        source -> Checkpoint[TimeStamp, Long](records)
      }
    }
    checkpointsBySource.foreach {
      sourceAndCheckpoint =>
        checkpointManager.writeCheckpoint(sourceAndCheckpoint._1,
          sourceAndCheckpoint._2, OffsetSerDe)
    }

    offsetsByTimeAndSource = Map.empty[(Source, TimeStamp), Long]
    checkpointsBySource
  }

  def replay[T <: Serializable](consumer: KafkaConsumer, startTime: TimeStamp,
             msgDecoder: Decoder[T], msgFilter: TimeStampFilter,
             msgHandler: Message => Unit): Map[Source, Long] = {
    val startEndOffsets = loadStartEndOffsets(startTime)
    startEndOffsets.foreach {
      case (source, offsets) =>
        val (startOffset, endOffset) = offsets
        val topicAndPartition = TopicAndPartition(source.name, source.partition)
        LOG.info(s"replay messages for $topicAndPartition from ${startOffset} to ${endOffset}")
        consumer.setStartEndOffsets(topicAndPartition, startOffset, Some(endOffset))
        consumer.start(topicAndPartition)
    }
    startEndOffsets.map {
      case (source, offsets) =>
        val (startOffset, endOffset) = offsets
        val topicAndPartition = TopicAndPartition(source.name, source.partition)
        startOffset.to(endOffset) foreach {
          offset =>
            val (kafkaMsg, time) = consumer.takeNextMessage(topicAndPartition)
            if (kafkaMsg.offset != offset) {
              LOG.error(s"unexpected offset. expected: ${offset}; actual: ${kafkaMsg.offset}")
            }
            val message = Message(msgDecoder.fromBytes(kafkaMsg.msg), time)
            msgFilter.filter(message, startTime).map(msgHandler)
        }
        source -> (endOffset + 1)
    }
  }

  def close(): Unit = {
    checkpointManager.close()
  }

  private def loadStartEndOffsets(startTime: TimeStamp): Map[Source, (Long, Long)] = {
    checkpointManager.sourceAndCheckpoints(OffsetSerDe)
      .foldLeft(Map.empty[Source, (Long, Long)]) { (accum, iter) =>
        val (source, checkpoints) = iter
        val recordsToReplay = checkpoints.records.sortBy(_._2).dropWhile(_._1 < startTime)
        if (recordsToReplay.nonEmpty) {
          accum + (source -> (recordsToReplay.head._2, recordsToReplay.last._2))
        } else {
          accum
        }
    }
  }
}
