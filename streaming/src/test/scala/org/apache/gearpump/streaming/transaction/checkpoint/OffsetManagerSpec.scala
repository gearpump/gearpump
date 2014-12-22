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

import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.transaction.checkpoint.api.{CheckpointSerDe, Checkpoint, CheckpointManager, Source}
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalacheck.Gen
import org.scalactic.Equality
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{PropSpec, Matchers}

class OffsetManagerSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val sourceGen = for {
    n <- Gen.alphaStr
    p <- Gen.choose(1, 100)
  } yield new Source {
      val name: String = n
      val partition: Int = p
    }
  val timeStampGen: Gen[TimeStamp] = Gen.choose(0L, 100L)
  val smallOffsetGen: Gen[Long] = Gen.choose(0L, 100L)
  val largeOffsetGen: Gen[Long] = Gen.choose(200L, 1000L)

  val checkpointManager = mock[CheckpointManager[TimeStamp, Long]]
  doNothing().when(checkpointManager).writeCheckpoint(
    any(classOf[Source]),
    any(classOf[Checkpoint[TimeStamp, Long]]),
    any(classOf[CheckpointSerDe[TimeStamp, Long]]))


  property("OffsetManager should only record the smallest offset at a timestamp for a source") {
    forAll(sourceGen, timeStampGen, smallOffsetGen, largeOffsetGen) {
      (source: Source, time: TimeStamp, smallOffset: Long, largeOffset: Long) =>
        val offsetManager = new OffsetManager(checkpointManager)
        offsetManager.update(source, time, largeOffset) shouldBe true
        offsetManager.update(source, time, smallOffset) shouldBe true
        offsetManager.update(source, time, largeOffset) shouldBe false
    }
  }

  val timeAndOffsetGen = for {
    timestamp <- timeStampGen
    offset <- smallOffsetGen
  } yield (timestamp, offset)

  val sourceAndCheckpointGen = for {
    source <- sourceGen
    timeAndOffsets <- Gen.containerOf[List, (TimeStamp, Long)](timeAndOffsetGen) suchThat (_.size > 0)
  } yield (source, Checkpoint(timeAndOffsets.toMap.toList))

  val sourceAndCheckpointMapGen: Gen[Map[Source, Checkpoint[TimeStamp, Long]]] = {
    Gen.containerOf[List, (Source, Checkpoint[TimeStamp, Long])](sourceAndCheckpointGen).map(_.toMap)
  }

  property("OffsetManager should checkpoint updated offsets for sources at timestamps") {
    forAll(sourceAndCheckpointMapGen) {
      (sourceCheckpointMap: Map[Source, Checkpoint[TimeStamp, Long]]) =>
        val offsetManager = new OffsetManager(checkpointManager)
        sourceCheckpointMap.foreach(sourceAndCheckpoint => {
          sourceAndCheckpoint._2.records.foreach(timeAndOffset => {
            offsetManager.update(sourceAndCheckpoint._1, timeAndOffset._1, timeAndOffset._2)
          })
        })
        val expected = sourceCheckpointMap
          .mapValues[Checkpoint[TimeStamp, Long]](c => Checkpoint(c.records.sortBy(_._1)))
        val actual = offsetManager.checkpoint
        actual shouldEqual expected

    }
  }
}
