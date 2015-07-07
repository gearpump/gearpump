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
import java.util.concurrent.TimeUnit

import akka.actor._
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.metrics.Metrics
import org.apache.gearpump.metrics.Metrics.MetricType
import org.apache.gearpump.partitioner.{Partitioner}
import org.apache.gearpump.streaming.AppMasterToExecutor._
import org.apache.gearpump.streaming.ExecutorToAppMaster._
import org.apache.gearpump.streaming.ProcessorId
import org.apache.gearpump.streaming.executor.Executor.TaskLocationReady
import org.apache.gearpump.util.{ActorUtil, LogUtil, TimeOutScheduler, Util}
import org.apache.gearpump.{Message, TimeStamp}
import org.slf4j.Logger

class TaskActor(val taskId: TaskId, val taskContextData : TaskContextData, userConf : UserConfig, val task: TaskWrapper) extends Actor with ExpressTransport  with TimeOutScheduler{
  var upstreamMinClock: TimeStamp = 0L


  import org.apache.gearpump.streaming.task.TaskActor._
  import taskContextData._
  import org.apache.gearpump.streaming.Constants.{_}
  val config = context.system.settings.config

  val LOG: Logger = LogUtil.getLogger(getClass, app = appId, executor = executorId, task = taskId)

  //metrics
  private val metricName = s"app${appId}.processor${taskId.processorId}.task${taskId.index}"
  private val receiveLatency = Metrics(context.system).histogram(s"$metricName.receiveLatency")
  private val processTime = Metrics(context.system).histogram(s"$metricName.processTime")
  private val sendThroughput = Metrics(context.system).meter(s"$metricName.sendThroughput")
  private val receiveThroughput = Metrics(context.system).meter(s"$metricName.receiveThroughput")

  private val registerTaskTimout = config.getLong(GEARPUMP_STREAMING_REGISTER_TASK_TIMEOUT_MS)
  private val maxPendingMessageCount = config.getInt(GEARPUMP_STREAMING_MAX_PENDING_MESSAGE_COUNT)
  private val ackOnceEveryMessageCount =  config.getInt(GEARPUMP_STREAMING_ACK_ONCE_EVERY_MESSAGE_COUNT)

  private var life = taskContextData.life

  //latency probe
  import scala.concurrent.duration._
  import context.dispatcher
  final val LATENCY_PROBE_INTERVAL = FiniteDuration(1, TimeUnit.SECONDS)

  // clock report interval
  final val CLOCK_REPORT_INTERVAL = FiniteDuration(1, TimeUnit.SECONDS)

  // flush interval
  final val FLUSH_INTERVAL = FiniteDuration(100, TimeUnit.MILLISECONDS)

  private val queue : util.ArrayDeque[Any] = new util.ArrayDeque[Any](INITIAL_WINDOW_SIZE)

  private var subscriptions = List.empty[(Int, Subscription)]

  // securityChecker will be responsible of dropping messages from
  // unknown sources
  private val securityChecker  = new SecurityChecker(taskId, self)
  private[task] val sessionId = Util.randInt

  //report to appMaster with my address
  express.registerLocalActor(TaskId.toLong(taskId), self)

  final def receive : Receive = null

  task.setTaskActor(this)

  def onStart(startTime : StartTime) : Unit = {
    task.onStart(startTime)
  }

  def onNext(msg : Message) : Unit = task.onNext(msg)

  def onUnManagedMessage(msg: Any): Unit = task.receiveUnManagedMessage.apply(msg)

  def onStop() : Unit = task.onStop()

  def output(msg : Message) : Unit = {
    var count = 0
    this.subscriptions.foreach{ subscription =>
      count += subscription._2.sendMessage(msg)
    }
    sendThroughput.mark(count)
  }

  def sendLatencyProbeMessage: Unit = {
    val probe = LatencyProbe(System.currentTimeMillis())
    subscriptions.foreach(_._2.probeLatency(probe))
  }

  final override def postStop() : Unit = {
    onStop()
  }

  final override def preStart() : Unit = {

    val register = RegisterTask(taskId, executorId, local)
    LOG.info(s"$register")
    sendMsgWithTimeOutCallBack(appMaster, register, registerTaskTimout, registerTaskTimeOut())
    system.eventStream.subscribe(taskContextData.appMaster, classOf[MetricType])
    context.become(waitForStartClock orElse stashMessages)
  }

  private def registerTaskTimeOut(): Unit = {
    LOG.error(s"Task ${taskId} failed to register to AppMaster of application $appId")
    throw new RestartException
  }

  def minClockAtCurrentTask: TimeStamp = {
    this.subscriptions.foldLeft(task.stateClock.getOrElse(Long.MaxValue)){ (clock, subscription) =>
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
    case start@ StartClock(clock) =>

      LOG.info(s"received $start")

      this.upstreamMinClock = clock

      subscriptions = subscribers.map { subscriber =>
        (subscriber.processorId ,
          new Subscription(appId, executorId, taskId, subscriber, sessionId, this,
            maxPendingMessageCount, ackOnceEveryMessageCount))
      }

      subscriptions.foreach(_._2.start)

      context.system.scheduler.schedule(
        LATENCY_PROBE_INTERVAL, LATENCY_PROBE_INTERVAL, self, SendMessageProbe)

      context.become(handleMessages(doHandleMessage))

      // Put this as the last step so that the subscription is already initialized.
      // Message sending in current Task before onStart will not be delivered to
      // target
      onStart(new StartTime(clock))

      // clean up history message in the queue
      doHandleMessage()
  }

  def stashMessages: Receive = handleMessages(() => Unit)

  def handleMessages(handler: () => Unit): Receive = {
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
    case inputMessage: Message =>
      val messageAfterCheck = securityChecker.checkMessage(inputMessage, sender)
      messageAfterCheck match {
        case Some(msg) =>
          queue.add(msg)
          handler()
        case None =>
      }
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
            subscription.changeLife(subscriber.lifeTime)
          case None =>
            val subscription = new Subscription(appId, executorId, taskId, subscriber, sessionId, this,
              maxPendingMessageCount, ackOnceEveryMessageCount)
            subscription.start
            subscriptions :+= (subscriber.processorId, subscription)
        }
      }
      sender ! TaskChanged(taskId, dagVersion)

    case SendMessageProbe =>
      sendLatencyProbeMessage
    case LatencyProbe(timeStamp) =>
      receiveLatency.update(System.currentTimeMillis() - timeStamp)
    case send: SendMessageLoss =>
      LOG.info("received SendMessageLoss")
      throw new MsgLostException
    case other =>
      queue.add(other)
      handler()
  }

  def minClock: TimeStamp = {
    Math.max(life.birth, Math.min(upstreamMinClock, minClockAtCurrentTask))
  }

  def getUpstreamMinClock: TimeStamp = upstreamMinClock

  private def getSubscription(processorId: ProcessorId): Option[Subscription] = {
    subscriptions.find(_._1 == processorId).map(_._2)
  }
}

object TaskActor {
  val INITIAL_WINDOW_SIZE = 1024 * 16
  val CLOCK_SYNC_TIMEOUT_INTERVAL = 3 * 1000 //3 seconds

  // If the message comes from an unknown source, securityChecker will drop it
  class SecurityChecker(task_id: TaskId, self : ActorRef) {

    private val LOG: Logger = LogUtil.getLogger(getClass, task = task_id)

    private var receivedMsgCount = Map.empty[ActorRef, MsgCount]

    def generateAckResponse(ackRequest: AckRequest, sender: ActorRef): Ack = {
      if (receivedMsgCount.contains(sender)) {
        Ack(task_id, ackRequest.seq, receivedMsgCount.get(sender).get.num, ackRequest.sessionId)
      } else {
        if(ackRequest.seq == 0){ //We got the first AckRequest before the real messages
          receivedMsgCount += sender -> new MsgCount(0L)
          Ack(task_id, ackRequest.seq, 0, ackRequest.sessionId)
        } else {
          LOG.debug(s"task $task_id get unkonwn AckRequest $ackRequest from ${ackRequest.taskId}")
          null
        }
      }
    }

    // If the message comes from an unknown source, then drop it
    def checkMessage(message : Message, sender: ActorRef): Option[Message] = {
      if(sender.equals(self)){
        Some(message)
      } else if (!receivedMsgCount.contains(sender)) {
          // This is an illegal message,
        LOG.debug(s"Task $task_id received message before receive the first AckRequest")
        None
      } else {
        receivedMsgCount.get(sender).get.increment()
        Some(message)
      }
    }

    private class MsgCount(var num: Long){
      def increment() = num += 1
    }
  }

  case class SendAck(ack: Ack, targetTask: TaskId)

  case object SendMessageProbe

  case object FLUSH
}
