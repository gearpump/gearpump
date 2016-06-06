/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming.task

import org.slf4j.Logger

import org.apache.gearpump.google.common.primitives.Shorts
import org.apache.gearpump.partitioner.{MulticastPartitioner, Partitioner, UnicastPartitioner}
import org.apache.gearpump.streaming.AppMasterToExecutor.MsgLostException
import org.apache.gearpump.streaming.LifeTime
import org.apache.gearpump.streaming.task.Subscription._
import org.apache.gearpump.util.LogUtil
import org.apache.gearpump.{Message, TimeStamp}

/**
 * Manges the output and message clock for single downstream processor
 *
 * @param subscriber downstream processor
 * @param maxPendingMessageCount trigger flow control. Should be bigger than
 *                               maxPendingMessageCountPerAckRequest
 * @param ackOnceEveryMessageCount send on AckRequest to the target
 */
class Subscription(
    appId: Int,
    executorId: Int,
    taskId: TaskId,
    subscriber: Subscriber, sessionId: Int,
    transport: ExpressTransport,
    maxPendingMessageCount: Int = MAX_PENDING_MESSAGE_COUNT,
    ackOnceEveryMessageCount: Int = ONE_ACKREQUEST_EVERY_MESSAGE_COUNT) {

  assert(maxPendingMessageCount >= ackOnceEveryMessageCount)
  assert(maxPendingMessageCount < Short.MaxValue / 2)

  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId,
    executor = executorId, task = taskId)

  import subscriber.{parallelism, partitionerDescription, processorId}

  // Don't worry if this store negative number. We will wrap the Short
  private val messageCount: Array[Short] = new Array[Short](parallelism)
  private val pendingMessageCount: Array[Short] = new Array[Short](parallelism)
  private val candidateMinClockSince: Array[Short] = new Array[Short](parallelism)

  private val minClockValue: Array[TimeStamp] = Array.fill(parallelism)(Long.MaxValue)
  private val candidateMinClock: Array[TimeStamp] = Array.fill(parallelism)(Long.MaxValue)

  private var maxPendingCount: Short = 0

  private var life = subscriber.lifeTime

  private val partitioner = partitionerDescription.partitionerFactory.partitioner
  private val sendFn = partitioner match {
    case up: UnicastPartitioner =>
      (msg: Message) => {
        val partition = up.getPartition(msg, parallelism, taskId.index)
        sendMessage(msg, partition)
      }
    case mp: MulticastPartitioner =>
      (msg: Message) => {
        val partitions = mp.getPartitions(msg, parallelism, taskId.index)
        partitions.map(partition => sendMessage(msg, partition)).sum
      }
  }

  def changeLife(life: LifeTime): Unit = {
    this.life = life
  }

  def start(): Unit = {
    val ackRequest = InitialAckRequest(taskId, sessionId)
    transport.transport(ackRequest, allTasks: _*)
  }

  def sendMessage(msg: Message): Int = {
    sendFn(msg)
  }

  /**
   * Returns how many message is actually sent by this subscription
   *
   * @param msg  the message to send
   * @param partition  the target partition to send message to
   * @return 1 if success
   */
  def sendMessage(msg: Message, partition: Int): Int = {

    var count = 0
    // Only sends message whose timestamp matches the lifeTime
    if (partition != Partitioner.UNKNOWN_PARTITION_ID && life.contains(msg.timestamp)) {

      val targetTask = TaskId(processorId, partition)
      transport.transport(msg, targetTask)

      this.minClockValue(partition) = Math.min(this.minClockValue(partition), msg.timestamp)
      this.candidateMinClock(partition) = Math.min(this.candidateMinClock(partition), msg.timestamp)

      incrementMessageCount(partition, 1)

      if (messageCount(partition) % ackOnceEveryMessageCount == 0) {
        sendAckRequest(partition)
      }

      if (messageCount(partition) / maxPendingMessageCount !=
        (messageCount(partition) + ackOnceEveryMessageCount) / maxPendingMessageCount) {
        sendLatencyProbe(partition)
      }
      count = 1
      count
    } else {
      if (needFlush) {
        flush()
      }
      count = 0
      count
    }
  }

  private var lastFlushTime: Long = 0L
  private val FLUSH_INTERVAL = 5 * 1000 // ms
  private def needFlush: Boolean = {
    System.currentTimeMillis() - lastFlushTime > FLUSH_INTERVAL &&
      Shorts.max(pendingMessageCount: _*) > 0
  }

  private def flush(): Unit = {
    lastFlushTime = System.currentTimeMillis()
    allTasks.foreach { targetTaskId =>
      sendAckRequest(targetTaskId.index)
    }
  }

  private def allTasks: scala.collection.Seq[TaskId] = {
    (0 until parallelism).map { taskIndex =>
      TaskId(processorId, taskIndex)
    }
  }

  /**
   * Handles acknowledge message. Throw MessageLossException if required.
   *
   * @param ack acknowledge message received
   */
  def receiveAck(ack: Ack): Unit = {

    val index = ack.taskId.index

    if (ack.sessionId == sessionId) {
      if (ack.actualReceivedNum == ack.seq) {
        if ((ack.seq - candidateMinClockSince(index)).toShort >= 0) {
          if (ack.seq == messageCount(index)) {
            // All messages have been acked.
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
        LOG.error(s"Failed! received ack: $ack, received: ${ack.actualReceivedNum}, " +
          s"sent: ${ack.seq}, try to replay...")
        throw new MsgLostException
      }
    }
  }

  def minClock: TimeStamp = {
    minClockValue.min
  }

  def allowSendingMoreMessages(): Boolean = {
    maxPendingCount < maxPendingMessageCount
  }

  def sendAckRequestOnStallingTime(stallingTime: TimeStamp): Unit = {
    minClockValue.indices.foreach { i =>
      if (minClockValue(i) == stallingTime && pendingMessageCount(i) > 0
        && allowSendingMoreMessages) {
        sendAckRequest(i)
        sendLatencyProbe(i)
      }
    }
  }

  private def sendAckRequest(partition: Int): Unit = {
    // Increments more count for each AckRequest
    // to throttle the number of unacked AckRequest
    incrementMessageCount(partition, ackOnceEveryMessageCount)
    val targetTask = TaskId(processorId, partition)
    val ackRequest = AckRequest(taskId, messageCount(partition), sessionId)
    transport.transport(ackRequest, targetTask)
  }

  private def incrementMessageCount(partition: Int, count: Int): Unit = {
    messageCount(partition) = (messageCount(partition) + count).toShort
    pendingMessageCount(partition) = (pendingMessageCount(partition) + count).toShort
    updateMaxPendingCount()
  }

  private def updateMaxPendingCount(): Unit = {
    maxPendingCount = Shorts.max(pendingMessageCount: _*)
  }

  private def sendLatencyProbe(partition: Int): Unit = {
    val probeLatency = LatencyProbe(System.currentTimeMillis())
    val targetTask = TaskId(processorId, partition)
    transport.transport(probeLatency, targetTask)
  }
}

object Subscription {
  // Makes sure it is smaller than MAX_PENDING_MESSAGE_COUNT
  final val ONE_ACKREQUEST_EVERY_MESSAGE_COUNT = 100
  final val MAX_PENDING_MESSAGE_COUNT = 1000
}