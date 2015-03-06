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
import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.streaming.AppMasterToExecutor._
import org.apache.gearpump.streaming.ExecutorToAppMaster._
import org.apache.gearpump.streaming.executor.Executor.TaskLocationReady
import org.apache.gearpump.util.{LogUtil, TimeOutScheduler, Util}
import org.apache.gearpump.{Message, TimeStamp}
import org.slf4j.Logger

class TaskActor(val taskContextData : TaskContextData, userConf : UserConfig, val task: TaskWrapper) extends Actor with ExpressTransport  with TimeOutScheduler{

  import org.apache.gearpump.streaming.task.TaskActor._
  import taskContextData._

  val LOG: Logger = LogUtil.getLogger(getClass, app = appId, executor = executorId, task = taskId)

  //metrics
  private val metricName = s"app${appId}.processor${taskId.processorId}.task${taskId.index}"
  private val receiveLatency = Metrics(context.system).histogram(s"$metricName.receiveLatency")
  private val processTime = Metrics(context.system).histogram(s"$metricName.processTime")
  private val sendThroughput = Metrics(context.system).meter(s"$metricName.sendThroughput")
  private val receiveThroughput = Metrics(context.system).meter(s"$metricName.receiveThroughput")

  //latency probe
  import scala.concurrent.duration._
  import context.dispatcher
  final val LATENCY_PROBE_INTERVAL = FiniteDuration(5, TimeUnit.SECONDS)
  context.system.scheduler.schedule(LATENCY_PROBE_INTERVAL, LATENCY_PROBE_INTERVAL, self, SendMessageProbe)

  private val queue : util.ArrayDeque[Any] = new util.ArrayDeque[Any](INITIAL_WINDOW_SIZE)
  private var partitioner : MergedPartitioner = null

  private var outputTaskIds : Array[TaskId] = null
  private var flowControl : FlowControl = null
  private var clockTracker : ClockTracker = null

  private var unackedClockSyncTimestamp : TimeStamp = 0
  private var needSyncToClockService = false

  private var minClock : TimeStamp = 0L

  // securityChecker will be responsible of dropping messages from
  // unknown sources
  private val securityChecker  = new SecurityChecker(taskId, self)
  private[task] val sessionId = Util.randInt

  //report to appMaster with my address
  express.registerLocalActor(TaskId.toLong(taskId), self)

  final def receive : Receive = null

  task.setTaskActor(this)

  def onStart(startTime : StartTime) : Unit = task.onStart(startTime)

  def onNext(msg : Message) : Unit = task.onNext(msg)

  def onStop() : Unit = task.onStop()

  def output(msg : Message) : Unit = {
    if (null == outputTaskIds || outputTaskIds.length == 0) {
      return
    }

    val partitions = partitioner.getPartitions(msg)

    var start = 0

    sendThroughput.mark(partitions.length)

    while (start < partitions.length) {
      val partition = partitions(start)

      transport(msg, outputTaskIds(partition))
      val ackRequest = flowControl.sendMessage(partition)
      if (null != ackRequest) {
        transport(ackRequest, outputTaskIds(partition))
      }

      start = start + 1
    }
  }

  def sendLatencyProbeMessage: Unit = {
    val probe = LatencyProbe(System.currentTimeMillis())
    if (null != outputTaskIds) {
      outputTaskIds.foreach { taskId =>
        transport(probe, taskId)
      }
    }
  }

  final override def postStop() : Unit = {
    onStop()
  }

  final override def preStart() : Unit = {

    sendMsgWithTimeOutCallBack(appMaster, RegisterTask(taskId, executorId, local), 10, registerTaskTimeOut())
    system.eventStream.subscribe(taskContextData.appMaster, classOf[MetricType])

    val graph = dag.graph
    LOG.info(s"TaskInit... taskId: $taskId")
    val outDegree = dag.graph.outDegreeOf(taskId.processorId)

    if (outDegree > 0) {

      val edges = graph.outgoingEdgesOf(taskId.processorId)

      LOG.info(s"task: ${taskId} out degree is $outDegree, edge length: ${edges.length}")

      this.partitioner = edges.foldLeft(MergedPartitioner.empty) { (mergedPartitioner, nodeEdgeNode) =>
        val (_, partitioner, processorId) = nodeEdgeNode
        val taskParallism = dag.processors.get(processorId).get.parallelism
        mergedPartitioner.add(partitioner, taskParallism)
      }

      LOG.info(s"task: ${taskId} partitioner: $partitioner")

      outputTaskIds = edges.flatMap {nodeEdgeNode =>
        val (_, _, processorId) = nodeEdgeNode
        val taskParallism = dag.processors.get(processorId).get.parallelism

        LOG.info(s"get output taskIds, processorId: $processorId, parallism: $taskParallism")

        0.until(taskParallism).map { taskIndex =>
          TaskId(processorId, taskIndex)
        }
      }.toArray

    } else {
      //outer degree == 0
      this.partitioner = null
      this.outputTaskIds = Array.empty[TaskId]
    }

    this.flowControl = new FlowControl(taskId, outputTaskIds.length, sessionId)
    this.clockTracker = new ClockTracker(flowControl)

    context.become(waitForStartClock orElse stashMessages)
  }

  private def registerTaskTimeOut(): Unit = {
    LOG.error(s"Task ${taskId} failed to register to AppMaster of application ${appId}")
    throw new RestartException
  }

  private def tryToSyncToClockService() : Unit = {
    if (unackedClockSyncTimestamp == 0) {
      appMaster ! UpdateClock(taskId, clockTracker.minClockAtCurrentTask)
      needSyncToClockService = false
      unackedClockSyncTimestamp = System.currentTimeMillis()
    } else {
      val current = System.currentTimeMillis()
      if (current - unackedClockSyncTimestamp > CLOCK_SYNC_TIMEOUT_INTERVAL) {
        appMaster ! UpdateClock(taskId, clockTracker.minClockAtCurrentTask)
        needSyncToClockService = false
        unackedClockSyncTimestamp  = System.currentTimeMillis()
      } else {
        needSyncToClockService = true
      }
    }
  }

  private def doHandleMessage() : Unit = {
    var done = false

    var count = 0
    val start = System.currentTimeMillis()
    while (flowControl.allowSendingMoreMessages() && !done) {
      val msg = queue.poll()
      if (msg != null) {
        msg match {
          case SendAck(ack, targetTask) =>
            transport(ack, targetTask)
            LOG.debug(s"Sending ack back, target taskId: $taskId, my task: $taskId, received message: ${ack.actualReceivedNum}")
          case m : Message =>
            val updated = clockTracker.onProcess(m)
            if (updated) {
              tryToSyncToClockService()
            }

            count += 1
            onNext(m)
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

  private def sendFirstAckRequests() : Unit = {
    for(i <- 0 until outputTaskIds.length) {
      val firstAckRequest = AckRequest(taskId, Seq(i, 0), sessionId)
      transport(firstAckRequest, outputTaskIds(i))
    }
  }

  def waitForStartClock : Receive = {
    case StartClock(clock) =>
      onStart(new StartTime(clock))
      context.become(handleMessages)
      sendFirstAckRequests()
  }

  def stashMessages = stashAndHandleMessages(handlNow = false)

  def handleMessages = stashAndHandleMessages(handlNow = true)

  def stashAndHandleMessages(handlNow: Boolean) : Receive = {
    case ackRequest : AckRequest =>
      //enqueue to handle the ackRequest and send back ack later
      val ack = securityChecker.generateAckResponse(ackRequest, sender)
      if(null != ack){
        queue.add(SendAck(ack, ackRequest.taskId))
      }
    case ack: Ack =>
      if(flowControl.messageLossDetected(ack)){
        LOG.error(s"Failed! Some messages sent from actor ${taskId} to ${taskId} are lost, try to replay...")
        throw new MsgLostException
      }
      flowControl.receiveAck(ack)
      val updated = clockTracker.onAck(ack)
      if (updated) {
        tryToSyncToClockService()
      }
      if (handlNow) {
        doHandleMessage()
      }
    case inputMessage: Message =>
      val messageAfterCheck = securityChecker.checkMessage(inputMessage, sender)
      messageAfterCheck match {
        case Some(msg) =>

          val updatedMessage = clockTracker.onReceive(msg)
          queue.add(updatedMessage)
          if (handlNow) {
            doHandleMessage()
          }
        case None =>
      }

    case ClockUpdated(timestamp) =>
      minClock = timestamp
      unackedClockSyncTimestamp = 0
      if (needSyncToClockService) {
        tryToSyncToClockService()
      }
    case RestartTask =>
      LOG.info(s"Restarting myself ${taskId} ")
      express.unregisterLocalActor(TaskId.toLong(taskId))
      throw new RestartException
    case TaskLocationReady =>
      sendLater.sendAllPendingMsgs()

    case SendMessageProbe =>
      if (handlNow) {
        // when the connection to downstream is established
        sendLatencyProbeMessage
      }
    case LatencyProbe(timeStamp) =>
      receiveLatency.update(System.currentTimeMillis() - timeStamp)
    case other =>
      LOG.error("Failed! Received unknown message " + "taskId: " + taskId + ", " + other.toString)
  }
}

object TaskActor {

  case object RestartTask

  val INITIAL_WINDOW_SIZE = 1024 * 16
  val CLOCK_SYNC_TIMEOUT_INTERVAL = 3 * 1000 //3 seconds

  class MergedPartitioner(partitioners : Array[Partitioner], partitionStart : Array[Int], partitionStop : Array[Int]) {

    def length = partitioners.length

    override def toString = {

      partitioners.mkString("partitioners: ", ",", "") + "\n" + partitionStart.mkString("start partitions:" , "," ,"") + "\n" + partitionStop.mkString("stopPartitions:" , "," ,"")

    }

    def add(partitioner : Partitioner, partitionNum : Int) = {
      val newPartitionStart = if (partitionStart.isEmpty) Array[Int](0) else { partitionStart :+ partitionStop.last }
      val newPartitionEnd = if (partitionStop.isEmpty) Array[Int](partitionNum) else {partitionStop :+ (partitionStop.last + partitionNum)}
      new MergedPartitioner(partitioners :+ partitioner, newPartitionStart, newPartitionEnd)
    }

    def getPartitions(msg : Message) : Array[Int] = {
      var start = 0
      val length = partitioners.length
      val result = new Array[Int](length)
      while (start < length) {
        result(start) = partitioners(start).getPartition(msg, partitionStop(start) - partitionStart(start)) + partitionStart(start)
        start += 1
      }
      result
    }
  }

  object MergedPartitioner {
    def empty = new MergedPartitioner(Array.empty[Partitioner], Array.empty[Int], Array.empty[Int])
  }

  // If the message comes from an unknown source, securityChecker will drop it
  class SecurityChecker(task_id: TaskId, self : ActorRef) {

    private val LOG: Logger = LogUtil.getLogger(getClass, task = task_id)

    private var receivedMsgCount = Map.empty[ActorRef, MsgCount]

    def generateAckResponse(ackRequest: AckRequest, sender: ActorRef): Ack = {
      if (receivedMsgCount.contains(sender)) {
        Ack(task_id, ackRequest.seq, receivedMsgCount.get(sender).get.num, ackRequest.sessionId)
      } else {
        if(ackRequest.seq.seq == 0){ //We got the first AckRequest before the real messages
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
}
