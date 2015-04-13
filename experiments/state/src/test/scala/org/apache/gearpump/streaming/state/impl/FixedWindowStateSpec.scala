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

import com.twitter.bijection.Injection
import org.apache.gearpump.{Message, TimeStamp}
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.state.api.{Window, CheckpointManager, StateOp}
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.{PropSpec, Matchers}
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks

class FixedWindowStateSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val windowSizeGen = Gen.chooseNum[Long](10, 100)
  val windowNumGen = Gen.chooseNum[Int](1, 10)
  val timeStampGen = Gen.chooseNum[Long](1, 1000)
  property("FixedWindowState should init a new window") {
    val initValueGen = Gen.chooseNum[Long](1, 1000)

    forAll(windowSizeGen, timeStampGen, initValueGen) { (windowSize: Long, startTime: TimeStamp, initValue: Long) =>
      val op = mock[StateOp[Long]]
      val checkpointManager = mock[CheckpointManager]
      val taskContext = MockUtil.mockTaskContext

      when(op.init).thenReturn(initValue)
      val fixedWindowState = new FixedWindowState[Long](windowSize, op, checkpointManager, taskContext)

      fixedWindowState.init(startTime)
      fixedWindowState.minClock shouldBe startTime
      fixedWindowState.unwrap shouldBe Some(initValue)
      verifyZeroInteractions(checkpointManager)
    }
  }


  property("FixedWindowState should get max clock of all windows") {
    forAll(windowSizeGen, windowNumGen) { (windowSize: Long, windowNum: Int) =>
      val op = StateUtil.count
      val checkpointManager = mock[CheckpointManager]
      val taskContext = MockUtil.mockTaskContext
      val fixedWindowState = new FixedWindowState[Long](windowSize, op, checkpointManager, taskContext)

      when(taskContext.upstreamMinClock).thenReturn(0L)
      when(checkpointManager.shouldCheckpoint).thenReturn(false)
      fixedWindowState.init(0L)

      0.until(windowNum) foreach { i =>
        fixedWindowState.update(Message(i + "", i * windowSize))
      }

      fixedWindowState.getMaxClock shouldBe Some(windowNum * windowSize)
    }
  }

  property("FixedWindowState should get window for a timestamp") {
    val windowIndexGen = Gen.chooseNum[Int](1, 10)
    forAll(windowSizeGen, windowIndexGen) { (windowSize: Long, windowIndex: Int) =>
      val op = StateUtil.count
      val checkpointManager = mock[CheckpointManager]
      val taskContext = MockUtil.mockTaskContext
      val fixedWindowState = new FixedWindowState[Long](windowSize, op, checkpointManager, taskContext)
      val startClock = windowIndex * windowSize
      val endClock = (windowIndex + 1) * windowSize
      val window = Window(startClock, endClock)

      val timeGen = Gen.chooseNum[Long](startClock, endClock - 1)

      forAll(timeGen) { (time: TimeStamp) =>
        fixedWindowState.getWindow(time) shouldBe window
      }
    }
  }

  property("FixedWindowState should update state by window") {
    forAll(windowSizeGen, windowNumGen) { (windowSize: Long, windowNum: Int) =>
      val op = StateUtil.count
      val checkpointManager = mock[CheckpointManager]
      val taskContext = MockUtil.mockTaskContext
      val fixedWindowState = new FixedWindowState[Long](windowSize, op, checkpointManager, taskContext)

      when(taskContext.upstreamMinClock).thenReturn(0L)
      when(checkpointManager.shouldCheckpoint).thenReturn(false)

       0.until(windowNum) foreach { index =>
          0.until(index + 1) foreach { offset =>
            val timestamp = index * windowSize + offset
            fixedWindowState.update(Message(timestamp + "", timestamp))
          }
       }

      fixedWindowState.minClock shouldBe 0L
      fixedWindowState.unwrap shouldBe Some(windowNum)

      // first window expires
      val checkpointTime = windowSize
      val upstreamMinClock = windowSize
      when(taskContext.upstreamMinClock).thenReturn(upstreamMinClock)
      when(checkpointManager.shouldCheckpoint).thenReturn(true)
      when(checkpointManager.getCheckpointTime).thenReturn(Some(checkpointTime))

      fixedWindowState.update(Message(windowSize + "", windowSize * windowNum))

      verify(checkpointManager).checkpoint(checkpointTime, Injection[Long, Array[Byte]](1L))
      verify(taskContext).output(Message((checkpointTime, 1L), checkpointTime))
      fixedWindowState.minClock shouldBe checkpointTime
    }
  }

}
