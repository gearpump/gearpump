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

import com.google.common.primitives.{Shorts}
import org.apache.gearpump.streaming.AppMasterToExecutor.MsgLostException
import org.apache.gearpump.streaming.LifeTime
import org.apache.gearpump.streaming.task.Subscription._
import org.apache.gearpump.util.LogUtil
import org.apache.gearpump.{TimeStamp, Message}
import org.slf4j.Logger

/**
 *
 * This manage the output and message clock for single downstream processor
 *
 * @param subscriber downstream processor
 * @param maxPendingMessageCount trigger flow control. Should be bigger than maxPendingMessageCountPerAckRequest
 * @param ackOnceEveryMessageCount send on AckRequest to the target
 *
 */
class Subscription(
    appId: Int,
    executorId: Int,
    taskId: TaskId,
    subscriber: Subscriber, sessionId: Int,
    transport: ExpressTransport,
    maxPendingMessageCount: Int = MAX_PENDING_MESSAGE_COUNT,
    ackOnceEveryMessageCount: Int = ONE_ACKREQUEST_EVERY_MESSAGE_COUNT) {

  assert(maxPendingMessageCount > ackOnceEveryMessageCount)
  assert(maxPendingMessageCount  < Short.MaxValue / 2)

  val LOG: Logger = LogUtil.getLogger(getClass, app = appId, executor = executorId, task = taskId)

  import subscriber.{partitionerDescription, processorId, parallelism}

  // Don't worry if this store negative number. We will wrap the Short
  private val messageCount: Array[Short] = new Array[Short](parallelism)
  private val pendingMessageCount: Array[Short] = new Array[Short](parallelism)
  private val candidateMinClockSince: Array[Short] = new Array[Short](parallelism)

  private var minClockValue: Array[TimeStamp] = new Array[TimeStamp](parallelism)
  private var candidateMinClock: Array[TimeStamp] = new Array[TimeStamp](parallelism)

  private var maxPendingCount: Short = 0

  private var life = subscriber.lifeTime

  val partitioner = partitionerDescription.partitionerFactory.partitioner

  def changeLife(life: LifeTime): Unit = {
    this.life = life
  }

  def start: Unit = {
    val ackRequest = InitialAckRequest(taskId, sessionId)
    transport.transport(ackRequest, allTasks: _*)
  }

  /**
   * Return how many message is actually sent by this subscription
   * @param msg
   * @return
   */
  def sendMessage(msg: Message): Int = {

    // only send message whose timestamp matches the lifeTime
    if (life.contains(msg.timestamp)) {

      val partition = partitioner.getPartition(msg, parallelism, taskId.index)
      val targetTask = TaskId(processorId, partition)
      transport.transport(msg, targetTask)

      this.minClockValue(partition) = Math.min(this.minClockValue(partition), msg.timestamp)
      this.candidateMinClock(partition) = Math.min(this.candidateMinClock(partition), msg.timestamp)

      messageCount(partition) = (messageCount(partition) + 1).toShort
      pendingMessageCount(partition) = (pendingMessageCount(partition) + 1).toShort
      updateMaxPendingCount()

      if (messageCount(partition) % ackOnceEveryMessageCount == 0) {
        val ackRequest = AckRequest(taskId, messageCount(partition), sessionId)
        transport.transport(ackRequest, targetTask)
      }

      if (messageCount(partition) % maxPendingMessageCount == 0) {
        val probeLatency = LatencyProbe(System.currentTimeMillis())
        transport.transport(probeLatency, targetTask)
      }

      return 1
    } else {
      if (needFlush) {
        flush
      }

      return 0
    }
  }

  private var lastFlushTime: Long = 0L
  private val FLUSH_INTERVAL = 5 * 1000 // ms
  private def needFlush: Boolean = {
    System.currentTimeMillis() - lastFlushTime > FLUSH_INTERVAL && Shorts.max(pendingMessageCount: _*) > 0
  }

  private def flush: Unit = {
    lastFlushTime = System.currentTimeMillis()
    allTasks.foreach { targetTaskId =>
      val ackRequest = AckRequest(taskId, messageCount(targetTaskId.index), sessionId)
      transport.transport(ackRequest, targetTaskId)
    }
  }

  private def allTasks: scala.collection.Seq[TaskId] = {
    (0 until parallelism).map {taskIndex =>
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
        if ((ack.seq - candidateMinClockSince(index)).toShort >= 0) {
          if (ack.seq == messageCount(index)) {
            // all messages have been acked.
            minClockValue(index) = Long.MaxValue
          } else {
            minClockValue(index) = candidateMinClock(index)
          }
          candidateMinClock(index) = Long.MaxValue
          candidateMinClockSince(index) = messageCount(index)
        }

        pendingMessageCount(ack.taskId.index) = (messageCount(ack.taskId.index) - ack.seq).toShort
        updateMaxPendingCount()
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
    maxPendingCount < maxPendingMessageCount
  }

  private def updateMaxPendingCount() : Unit = {
    maxPendingCount = Shorts.max(pendingMessageCount: _*)
  }
}

object Subscription {
  //make sure it is smaller than MAX_PENDING_MESSAGE_COUNT
  final val ONE_ACKREQUEST_EVERY_MESSAGE_COUNT = 100
  final val MAX_PENDING_MESSAGE_COUNT = 1000
}