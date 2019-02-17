/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.streaming.task

import com.google.common.primitives.Shorts
import io.gearpump.{Message, Time}
import io.gearpump.Time.MilliSeconds
import io.gearpump.streaming.AppMasterToExecutor.MsgLostException
import io.gearpump.streaming.LifeTime
import io.gearpump.streaming.partitioner.{MulticastPartitioner, Partitioner, UnicastPartitioner}
import io.gearpump.streaming.source.Watermark
import io.gearpump.streaming.task.Subscription._
import io.gearpump.util.LogUtil
import org.slf4j.Logger

/**
 * Manages the output and message clock for single downstream processor
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
    subscriber: Subscriber,
    sessionId: Int,
    publisher: TaskActor,
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

  private val outputWatermark: Array[MilliSeconds] = Array.fill(parallelism)(
    Watermark.MIN.toEpochMilli)

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
    publisher.transport(ackRequest, allTasks: _*)
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
    if (partition != Partitioner.UNKNOWN_PARTITION_ID && life.contains(
      msg.timestamp.toEpochMilli)) {

      val targetTask = TaskId(processorId, partition)
      publisher.transport(msg, targetTask)

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

  private var lastFlushTime: Long = Time.MIN_TIME_MILLIS
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
        if (ack.watermark > outputWatermark(index)) {
          outputWatermark(index) = ack.watermark
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

  def watermark: MilliSeconds = {
    outputWatermark.min
  }

  def allowSendingMoreMessages(): Boolean = {
    maxPendingCount < maxPendingMessageCount
  }

  def onStallingTime(stallingTime: MilliSeconds): Unit = {
    outputWatermark.indices.foreach { i =>
      if (outputWatermark(i) == stallingTime &&
        pendingMessageCount(i) > 0 &&
        allowSendingMoreMessages) {
        sendAckRequest(i)
        sendLatencyProbe(i)
      } else if (publisher.getProcessingWatermark == Watermark.MAX &&
        pendingMessageCount(i) == 0) {
        outputWatermark(i) = Watermark.MAX.toEpochMilli
      }
    }
  }

  private def sendAckRequest(partition: Int): Unit = {
    // Increments more count for each AckRequest
    // to throttle the number of unacked AckRequest
    incrementMessageCount(partition, ackOnceEveryMessageCount)
    val targetTask = TaskId(processorId, partition)
    val processingWatermark = publisher.getProcessingWatermark.toEpochMilli
    val ackRequest = AckRequest(taskId, messageCount(partition), sessionId, processingWatermark)
    publisher.transport(ackRequest, targetTask)
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
    publisher.transport(probeLatency, targetTask)
  }
}

object Subscription {
  // Makes sure it is smaller than MAX_PENDING_MESSAGE_COUNT
  final val ONE_ACKREQUEST_EVERY_MESSAGE_COUNT = 100
  final val MAX_PENDING_MESSAGE_COUNT = 1000
}