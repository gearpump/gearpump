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

import java.util

import org.apache.gearpump.streaming.AppMasterToExecutor.MsgLostException
import org.apache.gearpump.streaming.task.Subscription._
import org.apache.gearpump.util.LogUtil
import org.apache.gearpump.{TimeStamp, Message}
import org.slf4j.Logger

/**
 *
 * This manage the output and message clock for single downstream processor
 *
 * @param subscriber downstream processor
 * @param sessionId
 * @param transport
 */
class Subscription(
    appId: Int,
    executorId: Int,
    taskId: TaskId,
    subscriber: Subscriber, sessionId: Int,
    transport: ExpressTransport) {

  val LOG: Logger = LogUtil.getLogger(getClass, app = appId, executor = executorId, task = taskId)

  import subscriber.{partitioner, processorId, processor}

  private var messageCount: Array[Long] = null
  private var pendingMessageCount: Array[Long] = null
  private var minClockValue: Array[TimeStamp] = null

  private var candidateMinClockSince: Array[Long] = null
  private var candidateMinClock: Array[TimeStamp] = null

  def start: Unit = {
    minClockValue = Array.fill(processor.parallelism)(Long.MaxValue)

    candidateMinClock = Array.fill(processor.parallelism)(Long.MaxValue)
    candidateMinClockSince = Array.fill(processor.parallelism)(0)

    messageCount = Array.fill(processor.parallelism)(0)
    pendingMessageCount = Array.fill(processor.parallelism)(0)
    val ackRequest = AckRequest(taskId, 0, sessionId)
    transport.transport(ackRequest, allTasks: _*)
  }

  def sendMessage(msg: Message): Unit = {

    val partition = partitioner.partitioner.getPartition(msg, processor.parallelism, taskId.index)
    val targetTask = TaskId(processorId, partition)
    transport.transport(msg, targetTask)

    this.minClockValue(partition) = Math.min(this.minClockValue(partition), msg.timestamp)

    this.candidateMinClock(partition) = Math.min(this.candidateMinClock(partition), msg.timestamp)

    messageCount(partition) += 1
    pendingMessageCount(partition) += 1

    if (messageCount(partition) % ONE_ACKREQUEST_PER_MESSAGE_COUNT == 0) {
      val ackRequest = AckRequest(taskId, messageCount(partition), sessionId)
      transport.transport(ackRequest, targetTask)
    }
  }

  def probeLatency(probe: LatencyProbe): Unit = {
    transport.transport(probe, allTasks: _*)
  }

  private def allTasks: scala.collection.Seq[TaskId] = {
    (0 until processor.parallelism).map {taskIndex =>
      TaskId(processorId, taskIndex)
    }
  }

  /**
   * throw MessageLossException if required.
   * @param ack
   */
  def receiveAck(ack: Ack): Unit = {

    val index = ack.taskId.index

    if (ack.sessionId == sessionId) {
      if (ack.actualReceivedNum == ack.seq) {
        if (ack.seq >= candidateMinClockSince(index)) {
          minClockValue(index) = candidateMinClock(index)
          candidateMinClock(index) = Long.MaxValue
          candidateMinClockSince(index) = messageCount(index)
        }

        pendingMessageCount(ack.taskId.index) = messageCount(ack.taskId.index) - ack.seq
      } else {
        LOG.error(s"Failed! Some messages sent from actor ${taskId} to ${taskId} are lost, try to replay...")
        throw new MsgLostException
      }
    }
  }

  def minClock: TimeStamp = {
    minClockValue.min
  }

  def allowSendingMoreMessages() : Boolean = {
    if (pendingMessageCount.max < MAX_PENDING_MESSAGE_COUNT) {
      true
    } else {
      false
    }
  }
}

object Subscription {
  //make sure it is smaller than MAX_PENDING_MESSAGE_COUNT
  final val ONE_ACKREQUEST_PER_MESSAGE_COUNT = 100
  final val MAX_PENDING_MESSAGE_COUNT = 1000
}
