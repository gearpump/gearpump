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

package org.apache.gearpump.streaming.task

import org.apache.gearpump.{TimeStamp, Message}

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{PropSpec, Matchers}
import org.scalatest.mock.MockitoSugar


class ClockTrackerSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val messageGen = for {
    msg <- Gen.alphaStr
    timestamp <- Gen.choose[Long](0L, Long.MaxValue)
  } yield Message(msg, timestamp)
  val messageListGen = Gen.listOf[Message](messageGen) suchThat (_.size > 0)

  property("ClockTracker should update min clock on process without outgoing messages") {
    forAll(messageListGen) { (messageList: List[Message]) =>
      val flowControl = mock[FlowControl]
      when(flowControl.allMessagesAcked).thenReturn(true)
      val clockTracker = new ClockTracker(flowControl)
      val (minClock, newMsgList) = updateMinClockOnReceive(clockTracker, messageList)
      clockTracker.minClockAtCurrentTask shouldBe minClock

      updateMinClockOnProcess(clockTracker, newMsgList, allMessageAcked = true)
      clockTracker.minClockAtCurrentTask shouldBe minClock
    }
  }

  val allMessageAckedGen = Gen.oneOf(true, false)
  val exceedOutputWaterMarkGen = Gen.oneOf(true, false)
  property("ClockTracker should update min clock on ack with outgoing messages") {
    forAll(messageListGen, allMessageAckedGen, exceedOutputWaterMarkGen) {
      (messageList: List[Message], allMessageAcked: Boolean, exceedOutputWaterMark: Boolean) =>
      val flowControl = mock[FlowControl]
      when(flowControl.allMessagesAcked).thenReturn(allMessageAcked)
      when(flowControl.snapshotOutputWaterMark()).thenReturn(Array.empty[Long])
      when(flowControl.isOutputWatermarkExceed(anyObject[Array[Long]]))
        .thenReturn(exceedOutputWaterMark)
      val clockTracker = new ClockTracker(flowControl)
      val ack = mock[Ack]

      // no message received yet, not update min clock
      clockTracker.onAck(ack) shouldBe false

      val (minClock, newMsgList) = updateMinClockOnReceive(clockTracker, messageList)
      clockTracker.minClockAtCurrentTask shouldBe minClock

      // no message processed yet, not update min clock
      clockTracker.onAck(ack) shouldBe false

      val firstMsgProcessed = updateMinClockOnProcess(clockTracker, newMsgList, allMessageAcked)
      if (allMessageAcked) {
        clockTracker.onAck(ack) shouldBe minClock != Long.MaxValue
        clockTracker.minClockAtCurrentTask shouldBe Long.MaxValue
      } else if (firstMsgProcessed) {
        clockTracker.onAck(ack) shouldBe exceedOutputWaterMark
        clockTracker.minClockAtCurrentTask shouldBe minClock
      } else {
        clockTracker.onAck(ack) shouldBe false
        clockTracker.minClockAtCurrentTask shouldBe minClock
      }
    }
  }

  /**
   * return min clock along with a new message list as head message is replaced on receive
   */
  private def updateMinClockOnReceive(clockTracker: ClockTracker, messageList: List[Message]): (TimeStamp, List[Message]) = {
    messageList.foldLeft((Long.MaxValue, List.empty[Message])) { (accum, iter) =>
      val (minClock, newMsgList) = accum
      val msg = iter
      clockTracker.minClockAtCurrentTask shouldBe minClock
      if (msg.timestamp != Message.noTimeStamp) {
        if (newMsgList.isEmpty) {
          val shadowMsg = clockTracker.onReceive(msg)
          shadowMsg.eq(msg) shouldBe false
          Math.min(minClock, shadowMsg.timestamp) -> (newMsgList :+ shadowMsg)
        } else {
          clockTracker.onReceive(msg) shouldBe msg
          Math.min(minClock, msg.timestamp) -> (newMsgList :+ msg)
        }
      } else {
        accum
      }
    }
  }

  /**
   * return whether first message has been processed
   */
  private def updateMinClockOnProcess(clockTracker: ClockTracker, messageList: List[Message], allMessageAcked: Boolean): Boolean = {
    messageList.foldLeft((false, false)) { (update, msg) =>
      val (minClockUpdated, firstMsgProcessed) = update
      if (msg.timestamp == Message.noTimeStamp) {
        clockTracker.onProcess(msg) shouldBe false
        (minClockUpdated, firstMsgProcessed)
      } else if (minClockUpdated) {
        clockTracker.onProcess(msg) shouldBe false
        (true, true)
      } else if (allMessageAcked) {
        clockTracker.onProcess(msg) shouldBe true
        (true, true)
      } else {
        clockTracker.onProcess(msg) shouldBe false
        (false, true)
      }
    }._2
  }
}
