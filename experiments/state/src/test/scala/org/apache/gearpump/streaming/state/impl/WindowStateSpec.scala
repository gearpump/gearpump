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

import org.apache.gearpump._
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.state.api.{StateSerializer, WindowDescription, CheckpointStore}
import org.apache.gearpump.streaming.state.lib.op.Count
import org.apache.gearpump.streaming.state.lib.serializer.{WindowStateSerializer, LongSerializer}
import org.mockito.{Matchers => MockitoMatchers}
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks
import scala.collection.immutable.TreeMap
import scala.concurrent.duration._

class WindowStateSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val longGen = Gen.chooseNum[Long](100L, 10000L)
  val windowSizeGen = longGen.map(_ milliseconds)
  val windowStrideGen = longGen.map(_ milliseconds)
  val windowDescriptionGen = for {
    windowSize <- windowSizeGen
    windowStride <- windowStrideGen
  } yield WindowDescription(windowSize, windowStride)

  property("WindowState should recover checkpointed state at given timestamp") {
    forAll(longGen, longGen, longGen, windowDescriptionGen) {
      (data: Long, checkpointInterval: Long,
       timestamp: TimeStamp, windowDescription: WindowDescription) =>
        val serializer = new LongSerializer
        val windowStateSerializer = new WindowStateSerializer(serializer)
        val checkpointStore = mock[CheckpointStore]
        val taskContext = MockUtil.mockTaskContext
        val state = new WindowState[Long](
          serializer, checkpointInterval, checkpointStore, taskContext, windowDescription)

        val checkpoint = TreeMap(timestamp -> data)
        val bytes = windowStateSerializer.serialize(checkpoint)
        when(checkpointStore.read(timestamp)).thenReturn(Some(bytes))

        state.recover(timestamp)

        state.minClock shouldBe timestamp
        state.getCheckpointTime shouldBe (timestamp + checkpointInterval)
        state.get shouldBe Some(checkpoint(timestamp))
        state.getCheckpointStates shouldBe checkpoint
    }
  }

  property("WindowState should update state and state clock") {
    forAll(longGen, longGen, windowDescriptionGen) {
      (data: Long, checkpointInterval: Long, windowDescription: WindowDescription) =>
        val serializer = new LongSerializer
        val windowStateSerializer = new WindowStateSerializer(serializer)
        val checkpointStore = mock[CheckpointStore]
        val taskContext = MockUtil.mockTaskContext
        val state = new WindowState[Long](serializer, checkpointInterval,
          checkpointStore, taskContext, windowDescription)
        val aggregate = new Count().aggregate
        val windowSize = windowDescription.size.toMillis
        val windowStride = windowDescription.stride.toMillis

        val t1 = 0L
        val s1 = TreeMap(0L -> data)
        // upstreamMinClock < checkpointTime
        when(taskContext.upstreamMinClock).thenReturn(t1)

        state.update(t1, data, aggregate)
        state.getMaxMessageTime shouldBe t1
        state.getCheckpointTime shouldBe checkpointInterval
        state.minClock shouldBe 0L
        state.get shouldBe s1.get(0L)

        val t2 = Math.min(Math.min(windowSize - 1, windowStride - 1), checkpointInterval - 1)
        val s2 = TreeMap(0L -> aggregate(data, data))
        // upstreamMinClock < checkpointTime
        when(taskContext.upstreamMinClock).thenReturn(t2)

        state.update(t2, data, aggregate)
        state.getMaxMessageTime shouldBe t2
        state.getCheckpointTime shouldBe checkpointInterval
        state.getCheckpointStates shouldBe s2
        state.minClock shouldBe 0L
        state.get shouldBe s2.get(0L)

        val t3 = windowStride
        val s3 = windowStride < windowSize match {
          case true =>
            TreeMap(0L -> aggregate(s2(0L), data), windowStride -> data)
          case false =>
            s2 + (windowStride -> data)
        }
        val cs3 = windowStride < checkpointInterval match {
          case true => s3
          case false => s2

        }

        // upstreamMinClock not updated for some reason
        when(taskContext.upstreamMinClock).thenReturn(t2)

        state.update(t3, data, aggregate)

        state.getMaxMessageTime shouldBe t3
        state.getCheckpointTime shouldBe checkpointInterval
        state.getCheckpointStates shouldBe cs3
        state.minClock shouldBe 0L
        state.get shouldBe s3.lastOption.map(_._2)

        val t4 = t3
        val s4 = windowStride < windowSize match {
          case true =>
            TreeMap(0L -> aggregate(s3(0L), data),
              windowStride -> aggregate(s3(windowStride), data))
          case false =>
            s3 + (windowStride -> aggregate(s3(windowStride), data))
        }

        when(taskContext.upstreamMinClock).thenReturn(t4)
        state.update(t4, data, aggregate)

        state.getMaxMessageTime shouldBe t4
        if (t4 >= checkpointInterval) {
          val bytes = windowStateSerializer.serialize(cs3)
          verify(checkpointStore).write(
            MockitoMatchers.eq(checkpointInterval),
            MockitoMatchers.eq(bytes)
          )
          val nextCheckpointTime = checkpointInterval +
              ((t4 - checkpointInterval) / checkpointInterval + 1) * checkpointInterval
          state.getCheckpointTime shouldBe  nextCheckpointTime
          state.minClock shouldBe checkpointInterval
          state.getCheckpointStates shouldBe s4.dropWhile(_._1 + windowSize <= checkpointInterval)
          state.get shouldBe s4.dropWhile(_._1 + windowSize <= checkpointInterval).lastOption.map(_._2)
        }
    }
  }

  property("WindowState should get corrent windows for timestamp") {
    forAll(longGen, windowDescriptionGen, longGen) {
      (checkpointInterval: Long, windowDescription: WindowDescription, timestamp: TimeStamp) =>
        val serializer = mock[StateSerializer[Long]]
        val checkpointStore = mock[CheckpointStore]
        val taskContext = MockUtil.mockTaskContext
        val state = new WindowState[Long](serializer, checkpointInterval,
          checkpointStore, taskContext, windowDescription)

        val size = windowDescription.size.toMillis
        val stride = windowDescription.stride.toMillis
        val windows = 0L.to(timestamp, stride).dropWhile(_ + size <= timestamp)

        state.getWindows(timestamp) shouldBe windows
    }
  }
}
