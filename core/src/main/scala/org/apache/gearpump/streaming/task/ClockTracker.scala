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

/**
 * Clocktracker will keep track of all pending messages on current task
 */
class ClockTracker(flowControl : FlowControl)  {
import ClockTracker._

  private var minClock : TimeStamp = Long.MaxValue
  private var candidateMinClock : MinClockSince = null

  private var newReceivedMsg : Message = null

  private var unprocessedMsgCount : Long = 0

  /**
   * This method may replace the msg with a new message.
   */
  def onReceive(msg: Message): Message = {
    if (msg.timestamp == Message.noTimeStamp) {
      return msg
    }

    newReceivedMsg = msg
    unprocessedMsgCount += 1
    minClock = Math.min(minClock, msg.timestamp)

    if (null == candidateMinClock) {
      /**
       * Create a shadow message for MinClockSince so that we can get a new
       * object reference, so that we can make sure the uniqueness of this
       * message. In MinClockSince, it will use the object.eq to test whether
       * received message equals the head of MinClockSince.
       *
       * For example, if we receive 3 same message(same object reference),
       * MinClockSince will still be able to differentiate these three messages
       * by testing object.eq.
       */
      val shadowMsg = msg.copy()
      candidateMinClock = new MinClockSince(shadowMsg, flowControl)
      shadowMsg
    } else {
      candidateMinClock.receiveNewMsg(msg)
      msg
    }
  }

  /**
   * return true if there are changes to self min clock
   */
  def onProcess(msg: Message): Boolean = {
    if (msg.timestamp == Message.noTimeStamp) {
      return false
    }

    unprocessedMsgCount -= 1

    if (candidateMinClock != null) {
      val newMinClock = candidateMinClock.processMsg(msg)
      if (newMinClock.isDefined) {
        minClock = newMinClock.get
        candidateMinClock = null
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  /**
   * return true if there are changes to self min clock
   */
  def onAck(ack: Ack): Boolean = {
    if (unprocessedMsgCount == 0 && flowControl.allMessagesAcked) {
      candidateMinClock = null
    if (minClock == Long.MaxValue) {
        false
      } else {
        minClock = Long.MaxValue
        true
      }
    } else if (null != candidateMinClock) {
      val newMinClock = candidateMinClock.ackMsg()
      if (newMinClock.isDefined) {
        minClock = newMinClock.get
        candidateMinClock = null
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  /**
   * min clock timestamp of all messages pending at current task
   */
  def minClockAtCurrentTask : TimeStamp = {
    minClock
  }
}

object ClockTracker {

  class MinClockSince(val head: Message, flow: FlowControl) {
    private var minClock = head.timestamp
    private var ackThreshold: Array[Long] = null

    private var firstMsgProcessed = false

    def receiveNewMsg(msg: Message): Unit = {
      minClock = Math.min(minClock, msg.timestamp)
    }

    def processMsg(msg: Message): Option[TimeStamp] = {
      if (flow.allMessagesAcked) {
        Some(minClock)
      } else if (!firstMsgProcessed && msg.eq(this.head)) {
        ackThreshold = flow.snapshotOutputWaterMark()
        firstMsgProcessed = true
        None
      } else {
        None
      }
    }

    def ackMsg(): Option[TimeStamp] = {
      if (firstMsgProcessed) {
        if (flow.isOutputWatermarkExceed(ackThreshold)) {
          Some(minClock)
        } else {
          None
        }
      } else {
        None
      }
    }
  }
}