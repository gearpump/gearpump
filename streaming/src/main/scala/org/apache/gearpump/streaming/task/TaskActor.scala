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

import java.time.Instant
import java.util
import java.util.concurrent.TimeUnit

import akka.actor._
import com.gs.collections.impl.map.mutable.primitive.IntShortHashMap
import org.apache.gearpump.Message
import org.apache.gearpump.Time.MilliSeconds
import org.apache.gearpump.streaming.source.Watermark
import org.slf4j.Logger
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.metrics.Metrics
import org.apache.gearpump.serializer.SerializationFramework
import org.apache.gearpump.streaming.AppMasterToExecutor._
import org.apache.gearpump.streaming.Constants._
import org.apache.gearpump.streaming.ExecutorToAppMaster._
import org.apache.gearpump.streaming.ProcessorId
import org.apache.gearpump.streaming.task.TaskActor._
import org.apache.gearpump.util.{LogUtil, TimeOutScheduler}

import scala.collection.JavaConverters._
import scala.concurrent.duration._


/**
 * All tasks of Gearpump run inside an Actor. TaskActor is the Actor container for a task.
 */
class TaskActor(
    val taskId: TaskId,
    val taskContextData: TaskContextData,
    userConf: UserConfig,
    val task: TaskWrapper,
    inputSerializerPool: SerializationFramework)
    extends Actor with ExpressTransport with TimeOutScheduler {
  final val LATENCY_PROBE_INTERVAL = FiniteDuration(1, TimeUnit.SECONDS)
  // Clock report interval
  final val CLOCK_REPORT_INTERVAL = FiniteDuration(100, TimeUnit.MILLISECONDS)
  // Flush interval
  final val FLUSH_INTERVAL = FiniteDuration(100, TimeUnit.MILLISECONDS)
  val LOG: Logger = LogUtil.getLogger(getClass,
    app = taskContextData.appId, executor = taskContextData.executorId, task = taskId)
  private val config = context.system.settings.config
  // Metrics
  private val metricName =
    s"app${taskContextData.appId}.processor${taskId.processorId}.task${taskId.index}"
  private val receiveLatency = Metrics(context.system).histogram(
    s"$metricName:receiveLatency", sampleRate = 1)
  private val processTime = Metrics(context.system).histogram(s"$metricName:processTime")
  private val sendThroughput = Metrics(context.system).meter(s"$metricName:sendThroughput")
  private val receiveThroughput = Metrics(context.system).meter(s"$metricName:receiveThroughput")
  private val maxPendingMessageCount = config.getInt(GEARPUMP_STREAMING_MAX_PENDING_MESSAGE_COUNT)
  private val ackOnceEveryMessageCount = config.getInt(
    GEARPUMP_STREAMING_ACK_ONCE_EVERY_MESSAGE_COUNT)
  private val executor = context.parent
  private val queue = new util.LinkedList[AnyRef]()
  // SecurityChecker will be responsible of dropping messages from
  // unknown sources
  private val securityChecker = new SecurityChecker(taskId, self)
  private val stashQueue = new util.LinkedList[MessageAndSender]()

  // Latency probe

  import context.dispatcher
  private var upstreamWatermark: Instant = Watermark.MIN
  private var processingWatermark: Instant = Watermark.MIN
  private var watermark: Instant = Watermark.MIN
  private var life = taskContextData.life
  private var subscriptions = List.empty[(Int, Subscription)]
  private[task] var sessionId = NONE_SESSION
  private var minClockReported: Boolean = true

  // Reports to appMaster with my address
  express.registerLocalActor(TaskId.toLong(taskId), self)

  final override def postStop(): Unit = {
    onStop()
  }

  task.setTaskActor(this)

  def onStop(): Unit = task.onStop()

  final override def preStart(): Unit = {
    val register = RegisterTask(taskId, taskContextData.executorId, local)
    LOG.info(s"$register")
    executor ! register
    context.become(waitForTaskRegistered)
  }

  def serializerPool: SerializationFramework = inputSerializerPool

  final def receive: Receive = null

  def onStart(startTime: Instant): Unit = {
    task.onStart(startTime)
  }

  def onNext(msg: Message): Unit = task.onNext(msg)

  def onUnManagedMessage(msg: Any): Unit = task.receiveUnManagedMessage.apply(msg)

  /**
   * output to a downstream by specifying a arrayIndex
   * @param arrayIndex this is not same as ProcessorId
   */
  def output(arrayIndex: Int, msg: Message): Unit = {
    var count = 0
    count += this.subscriptions(arrayIndex)._2.sendMessage(msg)
    sendThroughput.mark(count)
  }

  def output(msg: Message): Unit = {
    var count = 0
    this.subscriptions.foreach { subscription =>
      count += subscription._2.sendMessage(msg)
    }
    sendThroughput.mark(count)
  }

  def waitForTaskRegistered: Receive = {
    case TaskRegistered(_, id, startClock) =>
      this.sessionId = id
      context.become(waitForStartTask(startClock))
  }

  def waitForStartTask(startClock: MilliSeconds): Receive = {
    case start@StartTask(tid) =>
      assert(tid == this.taskId, s"$start sent to the wrong task ${this.taskId}")
      onStartTask(startClock)
    case other: AnyRef =>
      stashQueue.add(MessageAndSender(other, sender()))
  }

  def handleMessages(sender: => ActorRef): Receive = {
    case ackRequest: InitialAckRequest =>
      val ackResponse = securityChecker.handleInitialAckRequest(ackRequest)
      if (null != ackResponse) {
        queue.add(SendAck(ackResponse, ackRequest.taskId))
        doHandleMessage()
      }

    case ackRequest: AckRequest =>
      // Enqueue to handle the ackRequest and send back ack later
      val ackResponse = securityChecker.generateAckResponse(ackRequest, sender,
        ackOnceEveryMessageCount)
      if (null != ackResponse) {
        queue.add(SendAck(ackResponse, ackRequest.taskId))
        doHandleMessage()
      }

    case ack: Ack =>
      subscriptions.find(_._1 == ack.taskId.processorId).foreach(_._2.receiveAck(ack))
      doHandleMessage()

    case inputMessage: SerializedMessage =>
      val message = Message(serializerPool.get().deserialize(inputMessage.bytes),
        inputMessage.timeStamp)
      receiveMessage(message, sender)

    case inputMessage: Message =>
      receiveMessage(inputMessage, sender)

    case watermark@Watermark(instant) =>
      assert(sender.eq(self), "Watermark should only be sent from Task to itself")
      if (minClockReported) {
        onUpstreamMinClock(instant)
        minClockReported = false
      }
      receiveMessage(watermark.toMessage, sender)

    case UpstreamMinClock(upstreamClock) =>
      // 1. received from ClockService and report minClock back after CLOCK_REPORT_INTERVAL
      //    then ClockService will send another update and loop
      // 2. The loop is kicked off by GetUpstreamMinClock on start
      // 3. upstreamClock is None for source task since it's reported as watermark above
      //    by external source
      // 4. this is designed to avoid flooding the ClockService
      upstreamClock.foreach(clock => onUpstreamMinClock(Instant.ofEpochMilli(clock)))
      reportMinClock()

    case ChangeTask(_, dagVersion, newLife, subscribers) =>
      this.life = newLife
      subscribers.foreach { subscriber =>
        val processorId = subscriber.processorId
        val subscription = getSubscription(processorId)
        subscription match {
          case Some(subs) =>
            subs.changeLife(subscriber.lifeTime cross this.life)
          case None =>
            val subscription = new Subscription(taskContextData.appId,
              taskContextData.executorId, taskId, subscriber,
              sessionId, this, maxPendingMessageCount, ackOnceEveryMessageCount)
            subscription.start()
            subscriptions :+=(subscriber.processorId, subscription)
            // Sorting, keep the order
            subscriptions = subscriptions.sortBy(_._1)
        }
      }
      sender ! TaskChanged(taskId, dagVersion)

    case LatencyProbe(timeStamp) =>
      receiveLatency.update(System.currentTimeMillis() - timeStamp)

    case SendMessageLoss =>
      throw new MsgLostException

    case other: AnyRef =>
      queue.add(other)
      doHandleMessage()
  }

  /**
   * Returns min clock of upstream task
   */
  def getUpstreamMinClock: MilliSeconds = upstreamWatermark.toEpochMilli

  def getProcessingWatermark: Instant = processingWatermark

  def updateWatermark(watermark: Instant): Unit = {
    processingWatermark = TaskUtil.max(processingWatermark, watermark)
  }

  private def allowSendingMoreMessages(): Boolean = {
    subscriptions.forall(_._2.allowSendingMoreMessages())
  }

  private def doHandleMessage(): Int = {
    var done = false

    var count = 0

    while (allowSendingMoreMessages() && !done) {
      val msg = queue.poll()
      if (msg != null) {
        msg match {
          case SendAck(ack, targetTask) =>
            transport(ack, targetTask)
          case m: Message =>
            count += 1
            onNext(m)
          case other =>
            // un-managed message
            onUnManagedMessage(other)
        }
      } else {
        done = true
      }
    }

    count
  }

  private def onStartTask(startClock: MilliSeconds): Unit = {
    LOG.info(s"received start, clock: $startClock, sessionId: $sessionId")
    subscriptions = taskContextData.subscribers.map { subscriber =>
      (subscriber.processorId,
        new Subscription(taskContextData.appId, taskContextData.executorId, taskId, subscriber,
          sessionId, this, maxPendingMessageCount, ackOnceEveryMessageCount))
    }.sortBy(_._1)

    subscriptions.foreach(_._2.start())

    // Put this as the last step so that the subscription is already initialized.
    // Message sending in current Task before onStart will not be delivered to
    // target
    onStart(Instant.ofEpochMilli(startClock))

    stashQueue.asScala.foreach { item =>
      handleMessages(item.sender).apply(item.msg)
    }
    stashQueue.clear()

    taskContextData.appMaster ! GetUpstreamMinClock(taskId)
    context.become(handleMessages(sender))
  }

  private def receiveMessage(msg: Message, sender: ActorRef): Unit = {
    val messageAfterCheck = securityChecker.checkMessage(msg, sender)
    messageAfterCheck match {
      case Some(m) =>
        queue.add(m)
        val start = System.currentTimeMillis()
        val count = doHandleMessage()
        if (!self.eq(sender)) {
          receiveThroughput.mark(count)
        }
        if (count > 0) {
          processTime.update((System.currentTimeMillis() - start) / count)
        }
      case None =>
      // TODO: Indicate the error and avoid the LOG flood
      // LOG.error(s"Task $taskId drop message $msg")
    }
  }

  private def getSubscription(processorId: ProcessorId): Option[Subscription] = {
    subscriptions.find(_._1 == processorId).map(_._2)
  }

  /**
   * On receiving upstream min clock, this task will
   *
   *   1. update its upstreamMinClock and trigger watermark progress method
   *      if the new value is larger
   *   2. update its own min clock
   *   3. check for its own lifetime
   *
   * @param upstreamClock for DataSourceTask the clock comes from itself by DataSource.getWatermark
   *                      for other tasks, the clock comes from that reported to ClockService
   *                      by upstream tasks
   */
  private def onUpstreamMinClock(upstreamClock: Instant): Unit = {
    if (upstreamClock.isAfter(this.upstreamWatermark)) {
      this.upstreamWatermark = upstreamClock
      task.onWatermarkProgress(upstreamWatermark)
    }

    // For Task without subscriptions, this will be Watermark.MAX
    val subWatermark = getSubscriptionWatermark(subscriptions, watermark)

    watermark = TaskUtil.max(Instant.ofEpochMilli(life.birth),
      TaskUtil.min(upstreamWatermark,
        TaskUtil.min(processingWatermark, subWatermark)))

    // Checks whether current task is dead.
    if (watermark.toEpochMilli > life.death) {
      // There will be no more message received...
      val unRegister = UnRegisterTask(taskId, taskContextData.executorId)
      executor ! unRegister

      LOG.info(s"Sending $unRegister, current watermark: $watermark, life: $life")
    }
  }

  private def reportMinClock(): Unit = {
    val update = UpdateClock(taskId, watermark.toEpochMilli)
    context.system.scheduler.scheduleOnce(CLOCK_REPORT_INTERVAL) {
      taskContextData.appMaster ! update
      minClockReported = true
    }
  }

  private def getSubscriptionWatermark(subs: List[(Int, Subscription)], wmk: Instant): Instant = {
    Instant.ofEpochMilli(subs.foldLeft(Watermark.MAX.toEpochMilli) {
      case (min, (_, sub)) =>
        val subWmk = sub.watermark
        if (subWmk == wmk.toEpochMilli) {
          sub.onStallingTime(subWmk)
        }
        Math.min(min, subWmk)
    })
  }
}

object TaskActor {

  val NONE_SESSION: Int = -1

  // If the message comes from an unknown source, securityChecker will drop it
  class SecurityChecker(task_id: TaskId, self: ActorRef) {

    private val LOG: Logger = LogUtil.getLogger(getClass, task = task_id)

    // Uses mutable HashMap for performance optimization
    private val receivedMsgCount = new IntShortHashMap()

    def handleInitialAckRequest(ackRequest: InitialAckRequest): Ack = {
      LOG.debug(s"Handle InitialAckRequest for session $ackRequest")
      val sessionId = ackRequest.sessionId
      if (sessionId == NONE_SESSION) {
        LOG.error(s"SessionId is not initialized, ackRequest: $ackRequest")
        null
      } else {
        receivedMsgCount.put(sessionId, 0)
        Ack(task_id, 0, 0, sessionId, Watermark.MIN.toEpochMilli)
      }
    }

    def generateAckResponse(ackRequest: AckRequest, sender: ActorRef, incrementCount: Int): Ack = {
      val sessionId = ackRequest.sessionId
      if (receivedMsgCount.containsKey(sessionId)) {
        // Increments more count for each AckRequest
        // to throttle the number of unacked AckRequest
        receivedMsgCount.put(sessionId, (receivedMsgCount.get(sessionId) + incrementCount).toShort)
        Ack(task_id, ackRequest.seq, receivedMsgCount.get(sessionId), ackRequest.sessionId,
          ackRequest.watermark)
      } else {
        LOG.error(s"get unknown AckRequest $ackRequest from ${sender.toString()}")
        null
      }
    }

    // If the message comes from an unknown source, then drop it
    def checkMessage(message: Message, sender: ActorRef): Option[Message] = {
      if (sender.equals(self)) {
        Some(message)
      } else {
        val sessionId = getSessionId(sender)
        if (receivedMsgCount.containsKey(sessionId)) {
          receivedMsgCount.put(sessionId, (receivedMsgCount.get(sessionId) + 1).toShort)
          Some(message)
        } else {
          // This is an illegal message,
          LOG.debug(s"received message before receive the first AckRequest, session $sessionId")
          None
        }
      }
    }

    // Tricky performance optimization to save memory.
    // We store the session Id in the uid of ActorPath
    // ActorPath.hashCode is same as uid.
    private def getSessionId(actor: ActorRef): Int = {
      // TODO: As method uid is protected in [akka] package. We
      // are using hashCode instead of uid.
      actor.hashCode()
    }
  }

  case class SendAck(ack: Ack, targetTask: TaskId)

  case class MessageAndSender(msg: AnyRef, sender: ActorRef)

  case object FLUSH
}
