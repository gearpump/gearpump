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

package io.gearpump.streaming.task

import java.util
import java.util.concurrent.TimeUnit

import akka.actor._
import io.gearpump.cluster.UserConfig
import io.gearpump.gs.collections.impl.map.mutable.primitive.IntShortHashMap
import io.gearpump.metrics.Metrics
import io.gearpump.serializer.SerializerPool
import io.gearpump.streaming.AppMasterToExecutor.{TaskRejected, _}
import io.gearpump.streaming.ExecutorToAppMaster._
import io.gearpump.streaming.{Constants, ProcessorId}
import io.gearpump.streaming.executor.Executor.TaskLocationReady
import io.gearpump.util.{LogUtil, TimeOutScheduler}
import io.gearpump.{Message, TimeStamp}
import org.slf4j.Logger

class TaskActor(
    val taskId: TaskId,
    val taskContextData : TaskContextData,
    userConf : UserConfig,
    val task: TaskWrapper,
  inputSerializerPool: SerializerPool)
  extends Actor with ExpressTransport  with TimeOutScheduler{
  var upstreamMinClock: TimeStamp = 0L

  def serializerPool: SerializerPool = inputSerializerPool

  import Constants._
  import io.gearpump.streaming.task.TaskActor._
  import taskContextData._
  val config = context.system.settings.config

  val LOG: Logger = LogUtil.getLogger(getClass, app = appId, executor = executorId, task = taskId)

  //metrics
  private val metricName = s"app${appId}.processor${taskId.processorId}.task${taskId.index}"
  private val receiveLatency = Metrics(context.system).histogram(s"$metricName:receiveLatency", sampleRate = 1)
  private val processTime = Metrics(context.system).histogram(s"$metricName:processTime")
  private val sendThroughput = Metrics(context.system).meter(s"$metricName:sendThroughput")
  private val receiveThroughput = Metrics(context.system).meter(s"$metricName:receiveThroughput")

  private val registerTaskTimout = config.getLong(GEARPUMP_STREAMING_REGISTER_TASK_TIMEOUT_MS)
  private val maxPendingMessageCount = config.getInt(GEARPUMP_STREAMING_MAX_PENDING_MESSAGE_COUNT)
  private val ackOnceEveryMessageCount =  config.getInt(GEARPUMP_STREAMING_ACK_ONCE_EVERY_MESSAGE_COUNT)

  private var life = taskContextData.life

  //latency probe
  import context.dispatcher

  import scala.concurrent.duration._
  final val LATENCY_PROBE_INTERVAL = FiniteDuration(1, TimeUnit.SECONDS)

  // clock report interval
  final val CLOCK_REPORT_INTERVAL = FiniteDuration(1, TimeUnit.SECONDS)

  // flush interval
  final val FLUSH_INTERVAL = FiniteDuration(100, TimeUnit.MILLISECONDS)

  private val queue = new util.LinkedList[AnyRef]()

  private var subscriptions = List.empty[(Int, Subscription)]

  // securityChecker will be responsible of dropping messages from
  // unknown sources
  private val securityChecker  = new SecurityChecker(taskId, self)
  private[task] var sessionId = NONE_SESSION

  //report to appMaster with my address
  express.registerLocalActor(TaskId.toLong(taskId), self)

  final def receive : Receive = null

  private var taskLocationReady = false

  task.setTaskActor(this)

  def onStart(startTime : StartTime) : Unit = {
    task.onStart(startTime)
  }

  def onNext(msg : Message) : Unit = task.onNext(msg)

  def onUnManagedMessage(msg: Any): Unit = task.receiveUnManagedMessage.apply(msg)

  def onStop() : Unit = task.onStop()

  /**
   * output to a downstream by specifying a arrayIndex
   * @param arrayIndex, this is not same as ProcessorId
   * @param msg
   */
  def output(arrayIndex: Int, msg: Message) : Unit = {
    LOG.debug("[output]: " + msg.msg)
    var count = 0
    count +=  this.subscriptions(arrayIndex)._2.sendMessage(msg)
    sendThroughput.mark(count)
  }

  def output(msg : Message) : Unit = {
    LOG.debug("[output]: " + msg.msg)
    var count = 0
    this.subscriptions.foreach{ subscription =>
      count += subscription._2.sendMessage(msg)
    }
    sendThroughput.mark(count)
  }

  final override def postStop() : Unit = {
    onStop()
  }

  final override def preStart() : Unit = {

    val register = RegisterTask(taskId, executorId, local)
    LOG.info(s"$register")
    sendMsgWithTimeOutCallBack(appMaster, register, registerTaskTimout, registerTaskTimeOut())
    context.become(waitForStartClock orElse stashMessages)
  }

  private def registerTaskTimeOut(): Unit = {
    LOG.error(s"Task ${taskId} failed to register to AppMaster of application $appId")
    throw new RegisterTaskFailedException
  }

  def minClockAtCurrentTask: TimeStamp = {
    this.subscriptions.foldLeft(Long.MaxValue){ (clock, subscription) =>
      Math.min(clock, subscription._2.minClock)
    }
  }

  private def allowSendingMoreMessages(): Boolean = {
    subscriptions.forall(_._2.allowSendingMoreMessages())
  }

  private def doHandleMessage(): Unit = {
    var done = false

    var count = 0
    val start = System.currentTimeMillis()

    while (allowSendingMoreMessages() && !done) {
      val msg = queue.poll()
      if (msg != null) {
        msg match {
          case SendAck(ack, targetTask) =>
            transport(ack, targetTask)
            LOG.debug(s"Sending ack back, target taskId: $targetTask, my task: $taskId, received message: ${ack.actualReceivedNum}")
          case m : Message =>
            count += 1
            LOG.debug("[input]: " + m.msg)
            onNext(m)
          case other =>
            // un-managed message
            onUnManagedMessage(other)
        }
      } else {
        done = true
      }
    }

    receiveThroughput.mark(count)
    if (count > 0) {
      processTime.update((System.currentTimeMillis() - start) / count)
    }
  }

  def waitForStartClock : Receive = {
    case TaskRejected =>
      LOG.info(s"Task $taskId is rejected by AppMaster, shutting down myself...")
      context.stop(self)
    case start@ Start(clock, sessionId) =>
      this.sessionId = sessionId

      LOG.info(s"received $start")

      this.upstreamMinClock = clock

      subscriptions = subscribers.map { subscriber =>
        (subscriber.processorId ,
          new Subscription(appId, executorId, taskId, subscriber, sessionId, this,
            maxPendingMessageCount, ackOnceEveryMessageCount))
      }.sortBy(_._1)

      subscriptions.foreach(_._2.start)

      // clean up history message in the queue
      doHandleMessage()

      // Put this as the last step so that the subscription is already initialized.
      // Message sending in current Task before onStart will not be delivered to
      // target
      onStart(new StartTime(clock))

      if (taskLocationReady) {
        self ! TaskLocationReady
      }

      context.become(handleMessages(doHandleMessage))
    case TaskLocationReady =>
      this.taskLocationReady = true
  }

  def stashMessages: Receive = handleMessages(() => Unit)

  def handleMessages(handler: () => Unit): Receive = {
    case ackRequest: InitialAckRequest =>
      val ackResponse = securityChecker.handleInitialAckRequest(ackRequest)
      if (null != ackResponse) {
        queue.add(SendAck(ackResponse, ackRequest.taskId))
        handler()
      }
    case ackRequest: AckRequest =>
      //enqueue to handle the ackRequest and send back ack later
      val ackResponse = securityChecker.generateAckResponse(ackRequest, sender)
      if (null != ackResponse) {
        queue.add(SendAck(ackResponse, ackRequest.taskId))
        handler()
      }
    case ack: Ack =>
      subscriptions.find(_._1 == ack.taskId.processorId).foreach(_._2.receiveAck(ack))
      handler()
    case inputMessage: SerializedMessage =>
      val message = Message(serializerPool.get().deserialize(inputMessage.bytes), inputMessage.timeStamp)
      receiveMessage(message, sender(), handler)
    case inputMessage: Message =>
      receiveMessage(inputMessage, sender(), handler)
    case upstream@ UpstreamMinClock(upstreamClock) =>
      this.upstreamMinClock = upstreamClock
      val update = UpdateClock(taskId, minClock)
      context.system.scheduler.scheduleOnce(CLOCK_REPORT_INTERVAL) {
        appMaster ! update
      }
    case TaskLocationReady =>
      sendLater.sendAllPendingMsgs()
      LOG.info("TaskLocationReady, sending GetUpstreamMinClock to AppMaster ")
      appMaster ! GetUpstreamMinClock(taskId)

    case ChangeTask(_, dagVersion, life, subscribers) =>
      this.life = life
      subscribers.foreach { subscriber =>
        val processorId = subscriber.processorId
        val subscription = getSubscription(processorId)
        subscription match {
          case Some(subscription) =>
            subscription.changeLife(subscriber.lifeTime cross this.life)
          case None =>
            val subscription = new Subscription(appId, executorId, taskId, subscriber, sessionId, this,
              maxPendingMessageCount, ackOnceEveryMessageCount)
            subscription.start
            subscriptions :+= (subscriber.processorId, subscription)
            // resort, keep the order
            subscriptions = subscriptions.sortBy(_._1)
        }
      }
      sender ! TaskChanged(taskId, dagVersion)
    case LatencyProbe(timeStamp) =>
      receiveLatency.update(System.currentTimeMillis() - timeStamp)
    case send: SendMessageLoss =>
      LOG.info("received SendMessageLoss")
      throw new MsgLostException
    case other: AnyRef =>
      queue.add(other)
      handler()
  }

  def minClock: TimeStamp = {
    Math.max(life.birth, Math.min(upstreamMinClock, minClockAtCurrentTask))
  }

  def getUpstreamMinClock: TimeStamp = upstreamMinClock

  private def receiveMessage(msg: Message, sender: ActorRef, handler: () => Unit): Unit = {
    val messageAfterCheck = securityChecker.checkMessage(msg, sender)
    messageAfterCheck match {
      case Some(msg) =>
        queue.add(msg)
        handler()
      case None =>
    }
  }

  private def getSubscription(processorId: ProcessorId): Option[Subscription] = {
    subscriptions.find(_._1 == processorId).map(_._2)
  }
}

object TaskActor {
  val CLOCK_SYNC_TIMEOUT_INTERVAL = 3 * 1000 //3 seconds

  // If the message comes from an unknown source, securityChecker will drop it
  class SecurityChecker(task_id: TaskId, self : ActorRef) {

    private val LOG: Logger = LogUtil.getLogger(getClass, task = task_id)

    // Use mutable HashMap for performance optimization
    private val receivedMsgCount = new IntShortHashMap()


    // Tricky performance optimization to save memory.
    // We store the session Id in the uid of ActorPath
    // ActorPath.hashCode is same as uid.
    private def getSessionId(actor: ActorRef): Int = {
      //TODO: As method uid is protected in [akka] package. We
      // are using hashCode instead of uid.
      actor.hashCode()
    }

    def handleInitialAckRequest(ackRequest: InitialAckRequest): Ack = {
      LOG.debug(s"Handle InitialAckRequest for session $ackRequest" )
      val sessionId = ackRequest.sessionId
      if (sessionId == NONE_SESSION) {
        LOG.error(s"SessionId is not initialized, ackRequest: $ackRequest")
        null
      } else {
        receivedMsgCount.put(sessionId, 0)
        Ack(task_id, 0, 0, sessionId)
      }
    }

    def generateAckResponse(ackRequest: AckRequest, sender: ActorRef): Ack = {
      val sessionId = ackRequest.sessionId
      if (receivedMsgCount.containsKey(sessionId)) {
        Ack(task_id, ackRequest.seq, receivedMsgCount.get(sessionId), ackRequest.sessionId)
      } else {
        LOG.error(s"get unknown AckRequest $ackRequest from ${sender.toString()}")
        null
      }
    }

    // If the message comes from an unknown source, then drop it
    def checkMessage(message : Message, sender: ActorRef): Option[Message] = {
      if(sender.equals(self)){
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
  }

  case class SendAck(ack: Ack, targetTask: TaskId)

  case object FLUSH

  val NONE_SESSION = -1
}
