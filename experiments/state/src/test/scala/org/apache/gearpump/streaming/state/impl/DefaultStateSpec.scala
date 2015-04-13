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
import org.apache.gearpump.streaming.state.api.{CheckpointManager, StateOp}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

class DefaultStateSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val longGen = Gen.chooseNum[Long](1, 100)
  property("DefaultState should set state clock and init or recover state") {
    val checkpointGen = longGen.flatMap(l => Gen.oneOf[Option[Long]](None, Some(l)))
    forAll(longGen, longGen, checkpointGen) { (timestamp: TimeStamp, initValue: Long, checkpoint: Option[Long]) =>
      val stateOp = mock[StateOp[Long]]
      val checkpointManager = mock[CheckpointManager]
      val taskContext = MockUtil.mockTaskContext
      when(checkpointManager.recover(timestamp)).thenReturn(checkpoint.map(l => Injection[Long, Array[Byte]](l)))
      when(stateOp.init).thenReturn(initValue)
      checkpoint.foreach { l =>
        when(stateOp.deserialize(any[Array[Byte]])).thenReturn(l)
      }

      val defaultState = new DefaultState[Long](stateOp, checkpointManager, taskContext)
      defaultState.minClock shouldBe 0L
      defaultState.init(timestamp)
      defaultState.minClock shouldBe timestamp
      verify(checkpointManager).recover(timestamp)
      verify(stateOp).init
      checkpoint match {
        case Some(l) =>
          defaultState.unwrap shouldBe Some(l)
        case None =>
          defaultState.unwrap shouldBe Some(initValue)
      }
    }
  }

  property("DefaultState should update state and state clock") {
    val numGen = Gen.chooseNum[Long](500, 1000)
    val checkpointTimeGen = Gen.chooseNum[Long](1, 200)
    val intervalGen = Gen.chooseNum[Long](1, 10)
    forAll(numGen, checkpointTimeGen, intervalGen) { (num: Long, checkpointTime: TimeStamp, interval: Long) =>
        val stateOp = StateUtil.count
        val checkpointManager = mock[CheckpointManager]
        val taskContext = MockUtil.mockTaskContext
        val defaultState = new DefaultState[Long](stateOp, checkpointManager, taskContext)
        val nextCheckpointTime = checkpointTime + interval
        val next2CheckpointTime = nextCheckpointTime + interval

        // no checkpoint yet
        when(taskContext.upstreamMinClock).thenReturn(0L)
        when(checkpointManager.shouldCheckpoint).thenReturn(false)
        when(checkpointManager.getCheckpointTime).thenReturn(Some(checkpointTime))

        0L.until(num).foreach { i =>
          defaultState.update(Message(i + "", i))
        }
        verify(checkpointManager, never()).checkpoint(any[Long], any[Array[Byte]])

        // state clock should not advance
        defaultState.minClock shouldBe 0L
        // message with timestamp >= checkpointTime should not update state
        defaultState.unwrap shouldBe Some(checkpointTime)

        // all messages with timestamp < checkpointTime has been received by upstream
        // time to checkpoint state before checkpointTime
        when(taskContext.upstreamMinClock).thenReturn(checkpointTime)
        when(checkpointManager.shouldCheckpoint).thenReturn(true)
        when(checkpointManager.getCheckpointTime).thenReturn(Some(checkpointTime), Some(nextCheckpointTime))

        defaultState.update(Message(num + "", next2CheckpointTime))

        verify(checkpointManager).checkpoint(checkpointTime, Injection[Long, Array[Byte]](checkpointTime))
        // state clock should be updated to checkpointTime after checkpoint
        defaultState.minClock shouldBe checkpointTime
        // state affected by message with timestamp in [checkpointTime, nextCheckpointTime)
        defaultState.unwrap shouldBe Some(nextCheckpointTime)

        // all messages with timestamp < nextCheckpointTime has been received by upstream
        // time to checkpoint state before nextCheckpointTime
        when(taskContext.upstreamMinClock).thenReturn(nextCheckpointTime)
        when(checkpointManager.shouldCheckpoint).thenReturn(true)
        when(checkpointManager.getCheckpointTime).thenReturn(Some(nextCheckpointTime), Some(next2CheckpointTime))

        defaultState.update(Message(num + "", next2CheckpointTime))

        verify(checkpointManager).checkpoint(nextCheckpointTime, Injection[Long, Array[Byte]](nextCheckpointTime))
        // state clock should advance to nextCheckpointTime
        defaultState.minClock shouldBe nextCheckpointTime
        // state affected by messages between [nextCheckpointTime, next2CheckpointTime)
        defaultState.unwrap shouldBe Some(next2CheckpointTime)
      }
  }


}
