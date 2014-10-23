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

import org.apache.gearpump.streaming.transaction.api.OffsetManager
import org.apache.gearpump.streaming.transaction.api.OffsetManager._
import org.apache.gearpump.streaming.transaction.api.CheckpointManager._
import org.apache.gearpump.streaming.transaction.kafka._
import org.apache.gearpump.streaming.transaction.kafka.KafkaConfig._
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.util.Configs
import org.specs2.mutable._
import org.specs2.mock._


class OffsetManagerSpec extends Specification with Mockito {
  "OffsetManager" should {
    "checkpoint updated timestamp and offsets for each source" in {

      val checkpointManager = mock[KafkaCheckpointManager[TimeStamp, Long]]
      val filter = mock[RelaxedTimeFilter]

      checkpointManager.writeCheckpoint(any[Source], any[Checkpoint[TimeStamp, Long]], any[CheckpointSerDe[TimeStamp, Long]]) answers {args => }

      val offsetManager = new OffsetManager(checkpointManager, filter)

      val updates: List[((Source, TimeStamp, Long), Boolean)] = List(
        ((KafkaSource("t1", 0), 0L, 0L), true), ((KafkaSource("t1", 0), 0L, 1L), false),
        ((KafkaSource("t1", 1), 0L, 0L), true), ((KafkaSource("t1", 1), 1L, 2L), true),
        ((KafkaSource("t2", 0), 0L, 0L), true), ((KafkaSource("t2", 0), 1L, 1L), true)
      )

      updates.foreach {
        entry =>
          val source = entry._1._1
          val timestamp = entry._1._2
          val offset = entry._1._3
          val updated = entry._2
          offsetManager.update(source, timestamp, offset) must beEqualTo(updated)
      }

      val expected: Map[Source, List[(TimeStamp, Long)]] = Map(
        KafkaSource("t1", 0) -> List((0L, 0L)),
        KafkaSource("t1", 1) -> List((0L, 0L), (1L, 2L)),
        KafkaSource("t2", 0) -> List((0L, 0L), (1L, 1L))
      )

      val actual = offsetManager.checkpoint.map {
        sourceAndCheckpoint =>
          val source = sourceAndCheckpoint._1
          val checkpoint = sourceAndCheckpoint._2
          source -> checkpoint.records
      }


      actual must beEqualTo(expected)
    }
  }
}

