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

package org.apache.gearpump.streaming.state.impl

import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.state.api.CheckpointStore
import org.apache.gearpump.streaming.state.lib.op.Count
import org.apache.gearpump.streaming.state.lib.serializer.LongSerializer
import org.scalacheck.Gen
import org.mockito.{Matchers => MockitoMatchers}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

class DefaultStateSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val longGen = Gen.chooseNum[Long](100L, System.currentTimeMillis())

  property("DefaultState should recover checkpointed state at given timestamp") {
    forAll(longGen, longGen, longGen) {
      (checkpoint: Long, checkpointInterval: Long, timestamp: TimeStamp) =>
        val serializer = new LongSerializer
        val checkpointStore = mock[CheckpointStore]
        val taskContext = MockUtil.mockTaskContext
        val state = new DefaultState[Long](
          serializer, checkpointInterval, checkpointStore, taskContext)

        val bytes = serializer.serialize(checkpoint)
        when(checkpointStore.read(timestamp)).thenReturn(Some(bytes))

        state.recover(timestamp)

        state.minClock shouldBe timestamp
        state.getCheckpointTime shouldBe (timestamp + checkpointInterval)
        state.get shouldBe Some(checkpoint)
        state.getCheckpointState shouldBe Some(checkpoint)
    }
  }

  property("DefaultState should update state and state clock") {
    forAll(longGen, longGen) {
      (data: Long, checkpointInterval: Long) =>
        val serializer = new LongSerializer
        val checkpointStore = mock[CheckpointStore]
        val taskContext = MockUtil.mockTaskContext
        val state = new DefaultState[Long](
          serializer, checkpointInterval, checkpointStore, taskContext)
        val aggregate = new Count().aggregate

        val t1 = 0L
        val s1 = Some(data)
        // upstreamMinClock < checkpointTime
        when(taskContext.upstreamMinClock).thenReturn(t1)

        state.update(t1, data, aggregate)
        state.getMaxMessageTime shouldBe t1
        state.getCheckpointTime shouldBe checkpointInterval
        state.getCheckpointState shouldBe s1
        state.minClock shouldBe 0L
        state.get shouldBe s1

        val t2 = checkpointInterval / 2
        val s2 = Some(aggregate(s1.get, data))
        // upstreamMinClock < checkpointTime
        when(taskContext.upstreamMinClock).thenReturn(t2)

        state.update(t2, data, aggregate)
        state.getMaxMessageTime shouldBe t2
        state.getCheckpointTime shouldBe checkpointInterval
        state.getCheckpointState shouldBe s2
        state.minClock shouldBe 0L
        state.get shouldBe s2

        val t3 = checkpointInterval
        val s3 = Some(aggregate(s2.get, data))
        // upstreamMinClock not updated for some reason
        when(taskContext.upstreamMinClock).thenReturn(t2)

        state.update(t3, data, aggregate)
        state.getMaxMessageTime shouldBe t3
        state.getCheckpointTime shouldBe checkpointInterval
        state.getCheckpointState shouldBe s2
        state.minClock shouldBe 0L
        state.get shouldBe s3

        val t4 = checkpointInterval + 1
        val s4 = Some(aggregate(s3.get, data))
        // upstreamMinClock >= checkpointTime
        // triggers checkpoint
        when(taskContext.upstreamMinClock).thenReturn(checkpointInterval)

        state.update(t4, data, aggregate)
        state.getMaxMessageTime shouldBe t4
        verify(checkpointStore).write(
          MockitoMatchers.eq(checkpointInterval),
          MockitoMatchers.eq(serializer.serialize(s2.get)))
        state.getCheckpointTime shouldBe  2 * checkpointInterval
        state.getCheckpointState shouldBe s4
        state.minClock shouldBe checkpointInterval
        state.get shouldBe s4
    }
  }
}
