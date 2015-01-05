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

import org.apache.gearpump.util.Util
import org.scalatest.{Matchers, WordSpec}

class FlowControlSpec extends WordSpec with Matchers {
  val sessionId = Util.randInt()
  val outputTasks = 2
  val taskId = TaskId(0, 0)
  val flowControl = new FlowControl(taskId, outputTasks, sessionId)

  "The Flow Control" should {
    "send a AckRequest after FLOW_CONTROL_RATE messages" in {
      for(i <- 1 to FlowControl.INITIAL_WINDOW_SIZE) {
        if(i % FlowControl.FLOW_CONTROL_RATE == 0) {
          assert(flowControl.sendMessage(0) == AckRequest(taskId, Seq(0, i), sessionId))
          assert(flowControl.sendMessage(1) == AckRequest(taskId, Seq(1, i), sessionId))
        } else {
          assert(flowControl.sendMessage(0) == null)
          assert(flowControl.sendMessage(1) == null)
        }
      }
      assert(!flowControl.allowSendingMoreMessages())
      assert(!flowControl.allMessagesAcked)
      val outputWaterMark = flowControl.snapshotOutputWaterMark()
      assert(outputWaterMark(0) == FlowControl.INITIAL_WINDOW_SIZE)
    }

    "detect message loss properly" in {
      assert(!flowControl.messageLossDetected(Ack(taskId, Seq(0, 100), 100, sessionId)))
      assert(flowControl.messageLossDetected(Ack(taskId, Seq(0, 100), 99, sessionId)))
      assert(flowControl.messageLossDetected(Ack(taskId, Seq(0, 100), 199, sessionId)))
      assert(!flowControl.messageLossDetected(Ack(taskId, Seq(0, 100), 99, sessionId + 1)))
    }

    "adjust the watermark when Ack received" in {
      assert(flowControl.isOutputWatermarkExceed(null))
      assert(flowControl.isOutputWatermarkExceed(Array.empty[Long]))
      assert(flowControl.isOutputWatermarkExceed(Array(0, 0)))
      assert(!flowControl.isOutputWatermarkExceed(Array(FlowControl.INITIAL_WINDOW_SIZE + 1, FlowControl.INITIAL_WINDOW_SIZE + 1)))
      assert(!flowControl.allMessagesAcked)
      flowControl.receiveAck(Ack(taskId, Seq(0, FlowControl.INITIAL_WINDOW_SIZE), FlowControl.INITIAL_WINDOW_SIZE, sessionId))
      flowControl.receiveAck(Ack(taskId, Seq(1, FlowControl.INITIAL_WINDOW_SIZE), FlowControl.INITIAL_WINDOW_SIZE, sessionId))
      assert(flowControl.allMessagesAcked)
    }
  }
}
