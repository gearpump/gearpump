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

class ClockTracker(flowControl : FlowControl)  {

  private final val INVALID : Long = -1

  private var upstreamMinClock : Long = INVALID

  private var myMinClock : Long = INVALID
  private var candidateMinClock : MinClockSince = null

  private var newReceivedMsg : Message = null

  private var unprocessedMsgCount : Long = 0

  /**
   * TODO: Swap the message with a new instance to make sure the message is unique
   */
  def onReceive(msg: Message): Unit = {
    newReceivedMsg = msg
    unprocessedMsgCount += 1
    if (this.myMinClock == INVALID) {
      myMinClock = msg.timestamp
    } else {
      myMinClock = Math.min(myMinClock, msg.timestamp)
    }

    if (null == candidateMinClock) {
      candidateMinClock = new MinClockSince(msg, flowControl)
    } else {
      candidateMinClock.receiveNewMsg(msg)
    }
  }

  /**
   * return true if there are changes to self min clock
   */
  def onProcess(msg: Message): Boolean = {
    unprocessedMsgCount -= 1

    if (candidateMinClock != null) {
      val newMinClock = candidateMinClock.processMsg(msg)
      if (newMinClock.isDefined) {
        myMinClock = newMinClock.get
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
    if (unprocessedMsgCount == 0 && flowControl.isOutputEmpty) {
      candidateMinClock = null
      myMinClock = Long.MaxValue
      true
    } else if (null != candidateMinClock) {
      val newMinClock = candidateMinClock.ackMsg()
      if (newMinClock.isDefined) {
        myMinClock = newMinClock.get
        candidateMinClock = null
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  def onUpstreamMinClockUpdate(clock : Long) : Unit = {
    upstreamMinClock = clock
  }

  /**
   * min timestamp of all messages at current task and all upstream tasks(recursive)
   */
  def minClock : Long = {
    Math.min(upstreamMinClock, myMinClock)
  }

  /**
   * min clock timestamp of all messages pending at current task
   */
  def selfMinClock : Long = {
    myMinClock
  }
}
