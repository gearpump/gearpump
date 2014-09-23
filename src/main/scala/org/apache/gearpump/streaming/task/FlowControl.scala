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

import org.slf4j.{LoggerFactory, Logger}
import org.apache.gearpump.streaming.task.FlowControl.LOG

class FlowControl(taskId : TaskId, partitionNum : Int) {
  private final val INITIAL_WINDOW_SIZE = 1024 * 16
  private final val FLOW_CONTROL_RATE = 100

  private[this] var outputWindow : Long = INITIAL_WINDOW_SIZE
  private val ackWaterMark = new Array[Long](partitionNum)
  private val outputWaterMark = new Array[Long](partitionNum)
  private val ackRequestWaterMark = new Array[Long](partitionNum)

  def markOutput(partitionId : Int) : AckRequest = {
    outputWaterMark(partitionId) += 1
    outputWindow -= 1

    if (outputWaterMark(partitionId) > ackRequestWaterMark(partitionId) + FLOW_CONTROL_RATE) {
      ackRequestWaterMark(partitionId) = outputWaterMark(partitionId)
      AckRequest(taskId, Seq(partitionId, outputWaterMark(partitionId)))
    } else {
      null
    }
  }

  def markAck(ackTask : TaskId, seq : Seq) : Unit = {
    LOG.debug("get ack from downstream, current: " + this.taskId + "downL: " + ackTask + ", seq: " + seq + ", windows: " + outputWindow)
    outputWindow += seq.seq - ackWaterMark(seq.id)
    ackWaterMark(seq.id) = seq.seq
  }

  def pass() : Boolean = outputWindow > 0

  def outputSnapshot() : Array[Long] = {
    outputWaterMark.clone
  }

  def isOutputEmpty : Boolean = {
    outputWindow == INITIAL_WINDOW_SIZE
  }

  def outputExceed(outputCheck : Array[Long]) : Boolean = {

    if (null == outputCheck || outputCheck.length == 0) {
      return true
    }

    var index = 0
    while(index < outputWaterMark.size) {
      if (outputWaterMark(index) < outputCheck(index)) {
        return false
      }
      index += 1
    }
    return true
  }
}

object FlowControl {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[FlowControl])
}