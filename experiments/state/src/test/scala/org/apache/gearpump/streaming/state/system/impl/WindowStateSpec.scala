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

package org.apache.gearpump.streaming.state.system.impl

import com.twitter.algebird.Group._
import com.twitter.bijection.Injection._
import org.apache.gearpump._
import org.apache.gearpump.streaming.MockUtil
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

import scala.collection.immutable.TreeMap

class WindowStateSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val longGen = Gen.chooseNum[Long](100L, 10000L)
  val group = longGroup
  val injection = long2BigEndian

  val intervalGen = for {
    st <- longGen
    et <- Gen.chooseNum[Long](st + 1, 100000L)
  } yield Interval(st, et)

  property("WindowState init should recover checkpointed state") {
    forAll(longGen, intervalGen, longGen, longGen) {
      (data: Long, interval: Interval, windowSize: Long, windowStep: Long) =>
        val windowManager = new Window(windowSize, windowStep)
        val taskContext = MockUtil.mockTaskContext
        val state = new WindowState[Long](group, taskContext, windowManager)

        val timestamp = interval.startTime
        val checkpoint = TreeMap(interval -> data)
        val bytes = StateSerializer.serialize(checkpoint)

        state.recover(timestamp, bytes)

        state.get shouldBe Some(group.plus(group.zero, data))
        state.getIntervalStates(interval.startTime, interval.endTime) shouldBe checkpoint
    }
  }

  property("WindowState updates state without checkpoint") {
    forAll(longGen, longGen, longGen) {
      (data: Long, windowSize: Long, windowStep: Long) =>
        val windowManager = new Window(windowSize, windowStep)
        val taskContext = MockUtil.mockTaskContext
        val state = new WindowState[Long](group, taskContext, windowManager)
        val checkpointTime = windowSize * 2

        when(taskContext.upstreamMinClock).thenReturn(0L)

        val timestamp1 = windowSize / 2
        state.update(timestamp1, data, checkpointTime)

        val interval1 = state.getInterval(timestamp1, checkpointTime)
        val value1 = group.plus(group.zero, data)
        state.get shouldBe Some(value1)
        state.getIntervalStates(0L, windowSize) shouldBe TreeMap(interval1 -> value1)

        when(taskContext.upstreamMinClock).thenReturn(windowSize)

        val timestamp2 = windowStep + 1
        state.update(timestamp2, data, checkpointTime)

        val interval2 = state.getInterval(timestamp2, checkpointTime)
        val value2 = group.plus(group.zero, data)
        if (timestamp1 < windowStep) {
          state.get shouldBe Some(value2)
        } else {
          state.get shouldBe Some(group.plus(value1, value2))
        }
        val states = state.getIntervalStates(0L, windowStep + windowSize)
        if (interval1 == interval2) {
          states shouldBe TreeMap(interval1 -> group.plus(value1, value2))
        } else {
          states shouldBe TreeMap(interval1 -> value1, interval2 -> value2)
        }
    }
  }

  property("WindowState updates state with checkpoint") {
    forAll(longGen, longGen, longGen) {
      (data: Long, windowSize: Long, windowStep: Long) =>
        val windowManager = new Window(windowSize, windowStep)
        val taskContext = MockUtil.mockTaskContext
        val state = new WindowState[Long](group, taskContext, windowManager)
        val checkpointTime = windowSize / 2


        when(taskContext.upstreamMinClock).thenReturn(0L)

        val timestamp1 = checkpointTime - 1
        state.update(timestamp1, data, checkpointTime)

        val interval1 = state.getInterval(timestamp1, checkpointTime)
        val value1 = group.plus(group.zero, data)
        state.get shouldBe Some(value1)
        state.getIntervalStates(0L, windowSize) shouldBe TreeMap(interval1 -> value1)

        when(taskContext.upstreamMinClock).thenReturn(0L)

        val timestamp2 = checkpointTime + 1
        state.update(timestamp2, data, checkpointTime)

        val interval2 = state.getInterval(timestamp2, checkpointTime)
        val value2 = group.plus(group.zero, data)
        state.get shouldBe Some(group.plus(value1, value2))
        state.getIntervalStates(0L, windowSize) shouldBe
          TreeMap(interval1 -> value1, interval2 -> value2)

        when(taskContext.upstreamMinClock).thenReturn(checkpointTime)

        val timestamp3 = timestamp2
        state.update(timestamp3, data, checkpointTime)

        val interval3 = interval2
        val value3 = group.plus(value2, data)
        state.get shouldBe Some(group.plus(value1, value3))
        state.getIntervalStates(0L, windowSize) shouldBe
          TreeMap(interval1 -> value1, interval3 -> value3)
    }
  }

  property("WindowState gets interval for timestamp") {
    forAll(longGen, longGen, longGen, longGen) {
      (timestamp: TimeStamp, checkpointTime: TimeStamp , windowSize: Long, windowStep: Long) =>
        val windowManager = new Window(windowSize, windowStep)
        val taskContext = MockUtil.mockTaskContext
        val state = new WindowState[Long](group, taskContext, windowManager)

        val interval = state.getInterval(timestamp, checkpointTime)
        intervalSpec(interval, timestamp, checkpointTime, windowSize, windowStep)

        val nextTimeStamp = interval.endTime
        val nextInterval = state.getInterval(nextTimeStamp, checkpointTime)
        intervalSpec(nextInterval, nextTimeStamp, checkpointTime, windowSize, windowStep)

        interval.endTime shouldBe nextInterval.startTime
    }

    def intervalSpec(interval: Interval, timestamp: TimeStamp,
                     checkpointTime: TimeStamp, windowSize: Long, windowStep: Long): Unit = {
      interval.startTime should be <= interval.endTime
      timestamp / windowStep * windowStep should (be <= interval.startTime)
      (timestamp - windowSize) / windowStep * windowStep should (be <= interval.startTime)
      (timestamp / windowStep + 1) * windowStep should (be >= interval.endTime)
      ((timestamp - windowSize) / windowStep + 1) * windowStep + windowSize should (be >= interval.endTime)
      checkpointTime should (be <= interval.startTime or be >= interval.endTime)
    }
  }
}
