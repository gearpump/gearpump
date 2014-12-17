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

import org.apache.gearpump.util.{Constants, Configs}
import org.slf4j.{Logger, LoggerFactory}

class FlowControl(taskId : TaskId, outputTaskCount : Int) {
  import org.apache.gearpump.streaming.task.FlowControl._

  private var outputWindow : Long = INITIAL_WINDOW_SIZE
  private val ackWaterMark = new Array[Long](outputTaskCount)
  private val outputWaterMark = new Array[Long](outputTaskCount)
  private val ackRequestWaterMark = new Array[Long](outputTaskCount)

  def sendMessage(messagePartition : Int) : AckRequest = {
    outputWaterMark(messagePartition) += 1
    outputWindow -= 1

    if (outputWaterMark(messagePartition) > ackRequestWaterMark(messagePartition) + FLOW_CONTROL_RATE) {
      ackRequestWaterMark(messagePartition) = outputWaterMark(messagePartition)
      AckRequest(taskId, Seq(messagePartition, outputWaterMark(messagePartition)))
    } else {
      null
    }
  }

  def receiveAck(sourceTask : TaskId, seq : Seq) : Unit = {
    LOG.debug("get ack from downstream, current: " + this.taskId + "downstream: " + sourceTask + ", seq: " + seq + ", windows: " + outputWindow)
    outputWindow += seq.seq - ackWaterMark(seq.id)
    ackWaterMark(seq.id) = seq.seq
  }

  /**
   * return true if we allow to output more messages
   */
  def allowSendingMoreMessages() : Boolean = outputWindow > 0

  def snapshotOutputWaterMark() : Array[Long] = {
    outputWaterMark.clone
  }

  def allMessagesAcked : Boolean = {
    outputWindow == INITIAL_WINDOW_SIZE
  }

  def isOutputWatermarkExceed(threshold : Array[Long]) : Boolean = {
    if (null == threshold || threshold.length == 0) {
      return true
    }

    var index = 0
    while(index < outputWaterMark.size) {
      if (outputWaterMark(index) < threshold(index)) {
        return false
      }
      index += 1
    }
    true
  }
}

object FlowControl {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[FlowControl])

  final val INITIAL_WINDOW_SIZE: Long= Configs.SYSTEM_DEFAULT_CONFIG.getLong(Constants.GEARPUMP_FLOWCONTROL_WINDOW_SIZE)
  final val FLOW_CONTROL_RATE: Int = Configs.SYSTEM_DEFAULT_CONFIG.getInt(Constants.GEARPUMP_FLOWCONTROL_RATE)
}