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

package org.apache.gearpump.streaming.transaction

import org.apache.gearpump.streaming.transaction.api.{Checkpoint, Source}
import org.apache.gearpump.streaming.transaction.kafka.KafkaSource
import org.apache.gearpump.TimeStamp
import org.specs2.mutable._

object OffsetManagerTest extends App {

  val offsetManager = new FakeOffsetManager

  val offsetsByTimeAndSource: Map[(Source, TimeStamp), Long] = Map(
    (KafkaSource("t1", 0), 0L) -> 0L, (KafkaSource("t1", 0), 0L) -> 1L,
    (KafkaSource("t1", 1), 0L) -> 0L, (KafkaSource("t1", 1), 1L) -> 2L,
    (KafkaSource("t2", 0), 0L) -> 0L, (KafkaSource("t2", 0), 1L)  -> 1L
  )
  val expected: Map[Source, Checkpoint] = Map(
    KafkaSource("t1", 0) -> Checkpoint(Map(0L -> 0L)),
    KafkaSource("t1", 1) -> Checkpoint(Map(0L -> 0L, 1L -> 2L)),
    KafkaSource("t2", 0) -> Checkpoint(Map(0L -> 0L, 1L -> 1L))
  )

  offsetsByTimeAndSource.foreach {
    entry =>
      val source = entry._1._1
      val timestamp = entry._1._2
      val offset = entry._2

      offsetManager.update(source, timestamp, offset)
  }

  offsetsByTimeAndSource.groupBy(_._1._1).foreach(entry => System.out.println(s"source (${entry._1.name}, ${entry._1.partition})"))

  val actual: Map[Source, Checkpoint] = offsetManager.checkpoint
  actual.foreach {
    sourceAndCheckpoint =>
      System.out.println(s"source (${sourceAndCheckpoint._1.name}, ${sourceAndCheckpoint._1.partition}); " +
        s"checkpoint ${sourceAndCheckpoint._2}")
  }
  System.out.println(s"do they match ? ${verify(expected, actual)}")


  def verify(expected: Map[Source, Checkpoint], actual: Map[Source, Checkpoint]): Boolean = {
    val diff = expected -- actual.keys
    diff.isEmpty
  }
}

class OffsetManagerSpec extends Specification {

}

class FakeOffsetManager {
  private var offsetsByTimeAndSource = Map.empty[(Source, TimeStamp), Long]

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
    checkpointsBySource
  }
}