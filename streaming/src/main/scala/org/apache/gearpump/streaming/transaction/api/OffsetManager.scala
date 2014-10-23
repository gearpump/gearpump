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

package org.apache.gearpump.streaming.transaction.api

import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.transaction.api.CheckpointManager._
import org.apache.gearpump.streaming.transaction.kafka.KafkaUtil._
import org.slf4j.{LoggerFactory, Logger}

object OffsetManager {
  object OffsetSerDe extends CheckpointSerDe[TimeStamp, Long] {
    override def toKeyBytes(timestamp: TimeStamp): Array[Byte] = longToByteArray(timestamp)

    override def toValueBytes(offset: Long): Array[Byte] = longToByteArray(offset)

    override def fromKeyBytes(bytes: Array[Byte]): TimeStamp = byteArrayToLong(bytes)

    override def fromValueBytes(bytes: Array[Byte]): Long = byteArrayToLong(bytes)
  }

  private val LOG: Logger = LoggerFactory.getLogger(classOf[OffsetManager])
}

class OffsetManager(checkpointManager: CheckpointManager[TimeStamp, Long],
                    filter: OffsetFilter) {
  import org.apache.gearpump.streaming.transaction.api.OffsetManager._

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

  def loadStartingOffsets(timestamp: TimeStamp): Map[Source, Long] = {
    LOG.info("loading start offsets...")
    checkpointManager.sourceAndCheckpoints(OffsetSerDe).foldLeft(Map.empty[Source, Long]) { (accum, iter) =>
      filter.filter(iter._2.records, timestamp) match {
        case Some((_, offset)) => accum + (iter._1 -> offset)
        case None => accum
      }
    }
  }

  def close(): Unit = {
    checkpointManager.close()
  }



}
