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

class MinClockSince(val first : Message, flow : FlowControl) {
  private var minClock = first.timestamp
  private var ackThreshold : Array[Long] = null

  private var firstMsgProcessed = false

  def receiveNewMsg(msg : Message) : Unit = {
    minClock = Math.min(minClock, msg.timestamp)
  }

  def processMsg(msg : Message) : Option[Long] = {
    if (flow.isOutputEmpty) {
      Some(minClock)
    } else if (!firstMsgProcessed && msg.eq(this.first)) {
      ackThreshold = flow.outputSnapshot()
      firstMsgProcessed = true
      None
    } else {
      None
    }
  }

  def ackMsg() : Option[Long] = {
    if (firstMsgProcessed) {
      if (flow.outputExceed(ackThreshold)) {
        Some(minClock)
      } else {
        None
      }
    } else {
      None
    }
  }
}

/**
 * track the minClock status of current task
 * minClock is the lowest timestamp of all messages in current task
 *
 * state machine, better modelled with actor?
 */
class ClockTracker(flowControl : FlowControl)  {

  private final val INVALID : Long = -1

  private var upstreamMinClock : Long = INVALID

  private var minClock : Long = INVALID
  private var candidateMinClock : MinClockSince = null

  private var newReceivedMsg : Message = null

  private var unprocessedMsgCount : Long = 0

  /**
   * TODO: Swap the message with a new instance to make sure the message is unique
   */
  def onReceive(msg: Message): Unit = {
    newReceivedMsg = msg
    unprocessedMsgCount += 1
    if (this.minClock == INVALID) {
      minClock = msg.timestamp
    } else {
      minClock = Math.min(minClock, msg.timestamp)
    }

    if (null == candidateMinClock) {
      candidateMinClock = new MinClockSince(msg, flowControl)
    } else {
      candidateMinClock.receiveNewMsg(msg)
    }
  }

  def onProcess(msg: Message): Option[Long] = {
    unprocessedMsgCount -= 1

    if (candidateMinClock != null) {
      val newMinClock = candidateMinClock.processMsg(msg)
      if (newMinClock.isDefined) {
        minClock = newMinClock.get
        candidateMinClock = null
        Some(minClock)
      } else {
        None
      }
    } else {
      None
    }
  }

  /**
   * return an updated min clock if there are changes
   */
  def onAck(ack: Ack): Option[Long] = {
    if (unprocessedMsgCount == 0 && flowControl.isOutputEmpty) {
      candidateMinClock = null
      minClock = Long.MaxValue
      Some(minClock)
    } else if (null != candidateMinClock) {
      val newMinClock = candidateMinClock.ackMsg()
      if (newMinClock.isDefined) {
        minClock = newMinClock.get
        candidateMinClock = null
        Some(minClock)
      } else {
        None
      }
    } else {
      None
    }
  }

  def onUpstreamMinClock(clock : Long) : Unit = {
    upstreamMinClock = clock
  }

  /**
   * min timestamp of myself + all upstream tasks
   */
  def recursiveMinClock : Long = {
    Math.min(upstreamMinClock, minClock)
  }

  /**
   * self min clock
   */
  def selfMinClock : Long = {
    minClock
  }
}
