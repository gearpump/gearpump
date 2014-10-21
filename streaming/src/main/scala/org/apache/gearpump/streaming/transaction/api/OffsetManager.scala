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
import org.apache.gearpump.streaming.transaction.kafka.KafkaConfig._
import org.apache.gearpump.util.Configs
import org.slf4j.{LoggerFactory, Logger}

object OffsetManager {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[OffsetManager])
}
class OffsetManager(conf: Configs) {
  import org.apache.gearpump.streaming.transaction.api.OffsetManager._

  private val config = conf.config
  private val filter = config.getCheckpointFilter
  private val checkpointManager =
    config.getCheckpointManagerFactory.getCheckpointManager(conf)
  private var sources: Array[Source] = null
  private var offsetsByTimeAndSource = Map.empty[(Source, TimeStamp), Long]

  def start(): Unit = {
    LOG.info("starting offsetManager...")
    checkpointManager.start()
  }

  def register(sources: Array[Source]) = {
    this.sources = sources
    checkpointManager.register(this.sources)
  }

  def update(source: Source, timestamp: TimeStamp, offset: Long) = {
    if (!offsetsByTimeAndSource.contains((source, timestamp))) {
      offsetsByTimeAndSource += (source, timestamp) -> offset
    }
  }

  def checkpoint: Map[Source, Checkpoint] = {
    val checkpointsBySource = offsetsByTimeAndSource
      .groupBy(_._1._1)
      .mapValues[Checkpoint](values => {
        Checkpoint(values.map(entry => (entry._1._2, entry._2)))
      })
    checkpointsBySource.foreach {
        sourceAndCheckpoint =>
          checkpointManager.writeCheckpoint(sourceAndCheckpoint._1,
          sourceAndCheckpoint._2)
    }

    offsetsByTimeAndSource = Map.empty[(Source, TimeStamp), Long]
    checkpointsBySource
  }

  def loadStartingOffsets(timestamp: TimeStamp): Map[Source, Long] = {
    LOG.info("loading start offsets...")
    sources.foldLeft(Map.empty[Source, Long]) { (accum, source) =>
      filter.filter(checkpointManager.readCheckpoint(source), timestamp, conf) match {
        case Some(offset) => accum + (source -> offset)
        case None => accum
      }
    }
  }

  def close(): Unit = {
    checkpointManager.close()
  }

}
