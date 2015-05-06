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

package org.apache.gearpump.streaming.state.internal.impl

import com.twitter.algebird.Monoid
import com.twitter.algebird.Monoid._
import com.twitter.bijection.Injection
import com.twitter.bijection.Injection._
import org.apache.gearpump.{Message, TimeStamp}
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.state.api.MonoidState
import org.apache.gearpump.streaming.state.internal.api.{CheckpointWindow, WindowManager, CheckpointManager}
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.{PropSpec, Matchers}
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks

import scala.collection.immutable.TreeMap

class WindowStateManagerSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {
  import org.apache.gearpump.streaming.state.internal.impl.WindowStateManagerSpec._

  val timestampGen = Gen.chooseNum[Long](1L, 1000L)
  val windowSizeGen = Gen.chooseNum[Long](1L, 1000L)
  property("WindowStateManager init with no windows found for timestamp") {
    forAll(timestampGen, windowSizeGen) { (timestamp: TimeStamp, windowSize: Long) =>
      val checkpointManager = mock[CheckpointManager]
      val windowManager = mock[WindowManager]
      val taskContext = MockUtil.mockTaskContext
      val stateManager = new WindowStateManager[Long](longState, windowSize, checkpointManager,
        windowManager, taskContext)

      when(windowManager.getWindows).thenReturn(Array.empty[CheckpointWindow])
      stateManager.init(timestamp)

      verify(windowManager).forwardTo(timestamp)
      stateManager.minClock shouldBe timestamp
      stateManager.getStates shouldBe empty
    }
  }

  property("WindowStateManager init with no checkpoint found") {
    forAll(timestampGen, windowSizeGen) { (timestamp: TimeStamp, windowSize: Long) =>
      val checkpointManager = mock[CheckpointManager]
      val windowManager = mock[WindowManager]
      val taskContext = MockUtil.mockTaskContext
      val window = CheckpointWindow(0L, timestamp)
      val stateManager = new WindowStateManager[Long](longState, windowSize, checkpointManager,
        windowManager, taskContext)

      when(windowManager.getWindows).thenReturn(Array(window))
      when(checkpointManager.recover(timestamp)).thenReturn(None)

      stateManager.init(timestamp)

      verify(windowManager).forwardTo(timestamp)
      stateManager.minClock shouldBe timestamp
      stateManager.getStates shouldBe TreeMap[CheckpointWindow, Long](window -> longState.init)
    }
  }

  property("WindowStateManager init state to checkpoint at the timestamp") {
    val stateGen = Gen.chooseNum[Long](0L, 1000L)
    forAll(timestampGen, windowSizeGen, stateGen) { (timestamp: TimeStamp, windowSize: Long, state: Long) =>
      val checkpointManager = mock[CheckpointManager]
      val windowManager = mock[WindowManager]
      val taskContext = MockUtil.mockTaskContext
      val window = CheckpointWindow(0L, timestamp)
      val stateManager = new WindowStateManager[Long](longState, windowSize, checkpointManager,
        windowManager, taskContext)

      when(windowManager.getWindows).thenReturn(Array(window))
      when(checkpointManager.recover(timestamp)).thenReturn(Some(longState.serialize(state)))

      stateManager.init(timestamp)

      verify(windowManager).forwardTo(timestamp)
      stateManager.minClock shouldBe timestamp
      stateManager.getStates shouldBe TreeMap[CheckpointWindow, Long](window -> state)
    }
  }

  val dataGen = Gen.chooseNum[Long](0L, 1000L)
  val msgDecoder = (message: Message) => {
    message.msg.asInstanceOf[String].toLong
  }


  property("WindowStateManager update state with no checkpoint or no window forward") {
    forAll(dataGen, timestampGen, timestampGen) {
      (data: Long, timestamp: TimeStamp, upstreamMinClock: TimeStamp) =>
        val message = Message(data + "", timestamp)
        val checkpointManager = mock[CheckpointManager]
        val windowSize = timestamp * 2
        val windowManager = mock[WindowManager]
        val taskContext = MockUtil.mockTaskContext
        val stateManager = new WindowStateManager[Long](longState, windowSize, checkpointManager,
          windowManager, taskContext)

        when(checkpointManager.shouldCheckpoint).thenReturn(false, false)
        when(windowManager.shouldForward).thenReturn(false, false)
        when(windowManager.getWindowSize).thenReturn(windowSize, windowSize)

        stateManager.update(message, msgDecoder, upstreamMinClock)

        val updatedState = longState.update(longState.init, data)
        stateManager.getStates shouldBe TreeMap(CheckpointWindow(0L, windowSize) -> updatedState)

        stateManager.update(message, msgDecoder, upstreamMinClock)

        verify(checkpointManager, times(2)).syncTime(upstreamMinClock)
        stateManager.getStates shouldBe TreeMap(CheckpointWindow(0L, windowSize) ->
          longState.update(updatedState, data))
    }
  }

  property("WindowStateManager update state with checkpoint and window forward") {
    forAll(dataGen, timestampGen) {
      (data: Long, timestamp: TimeStamp) =>
        val message = Message(data + "", timestamp)
        val checkpointManager = mock[CheckpointManager]
        val windowSize = timestamp * 2
        val windowManager = mock[WindowManager]
        val taskContext = MockUtil.mockTaskContext
        val stateManager = new WindowStateManager[Long](longState, windowSize, checkpointManager,
          windowManager, taskContext)
        val upstreamMinClock = windowSize
        val checkpointTime = windowSize

        when(checkpointManager.shouldCheckpoint).thenReturn(true)
        when(checkpointManager.getCheckpointTime).thenReturn(Some(checkpointTime))
        when(windowManager.shouldForward).thenReturn(true)
        when(windowManager.getWindowSize).thenReturn(windowSize)
        when(windowManager.getMergedWindow).thenReturn(Some(CheckpointWindow(0L, windowSize)), Some(CheckpointWindow(windowSize, windowSize * 2)))

        stateManager.update(message, msgDecoder, upstreamMinClock)

        val updatedState = longState.update(longState.init, data)
        stateManager.minClock shouldBe checkpointTime
        stateManager.getStates shouldBe empty
        verify(checkpointManager).syncTime(upstreamMinClock)
        verify(checkpointManager).checkpoint(windowSize, longState.serialize(updatedState))
        verify(taskContext).output(MockUtil.argMatch((msg: Message) => {
          msg.timestamp == windowSize
        }))
        verify(windowManager).forward()
    }
  }

}

object WindowStateManagerSpec {

  val longState = new MonoidState[Long] {
    override def monoid: Monoid[Long] = longMonoid

    override def injection: Injection[Long, Array[Byte]] = long2BigEndian
  }
}
