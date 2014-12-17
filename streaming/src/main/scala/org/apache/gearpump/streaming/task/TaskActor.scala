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

import akka.actor._
import org.apache.gearpump.metrics.Metrics
import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.streaming.AppMasterToExecutor.{GetStartClock, RestartException, RestartTasks, StartClock}
import org.apache.gearpump.streaming.ConfigsHelper._
import org.apache.gearpump.streaming.ExecutorToAppMaster._
import org.apache.gearpump.util.Configs
import org.apache.gearpump.{Message, TimeStamp}
import org.slf4j.{Logger, LoggerFactory}

abstract class TaskActor(conf : Configs) extends Actor with ExpressTransport {
  import org.apache.gearpump.streaming.task.TaskActor._

  private val appId = conf.appId
  protected val taskId : TaskId =  conf.taskId

  private val metricName = s"app$appId.task${taskId.groupId}_${taskId.index}"
  private val latencies = Metrics(context.system).histogram(s"$metricName.latency")
  private val throughput = Metrics(context.system).meter(s"$metricName.throughput")

  private val appMaster : ActorRef = conf.appMaster

  private val queue : util.ArrayDeque[Any] = new util.ArrayDeque[Any](FlowControl.INITIAL_WINDOW_SIZE.toInt)
  private var partitioner : MergedPartitioner = null

  private var outputTaskIds : Array[TaskId] = null
  private var flowControl : FlowControl = null
  private var clockTracker : ClockTracker = null

  private var unackedClockSyncTimestamp : TimeStamp = 0
  private var needSyncToClockService = false

  private var minClock : TimeStamp = 0L

  //report to appMaster with my address
  express.registerLocalActor(TaskId.toLong(taskId), self)

  final def receive : Receive = null

  def onStart(context : TaskContext) : Unit

  def onNext(msg : Message) : Unit

  def onStop() : Unit = {}

  def output(msg : Message) : Unit = {
    if (null == outputTaskIds || outputTaskIds.length == 0) {
      return
    }

    val partitions = partitioner.getPartitions(msg)

    var start = 0

    throughput.mark(partitions.length)

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

  final override def postStop() : Unit = {
    onStop()
  }

  final override def preStart() : Unit = {

    appMaster ! RegisterTask(taskId, conf.executorId, local)

    val graph = conf.dag.graph
    LOG.info(s"TaskInit... taskId: $taskId")
    val outDegree = conf.dag.graph.outDegreeOf(taskId.groupId)

    if (outDegree > 0) {

      val edges = graph.outgoingEdgesOf(taskId.groupId)

      LOG.info(s"task: $taskId out degree is $outDegree, edge length: ${edges.length}")

      this.partitioner = edges.foldLeft(MergedPartitioner.empty) { (mergedPartitioner, nodeEdgeNode) =>
        val (_, partitioner, taskgroupId) = nodeEdgeNode
        val taskParallism = conf.dag.tasks.get(taskgroupId).get.parallelism
        mergedPartitioner.add(partitioner, taskParallism)
      }

      LOG.info(s"task: $taskId partitioner: $partitioner")

      outputTaskIds = edges.flatMap {nodeEdgeNode =>
        val (_, _, taskgroupId) = nodeEdgeNode
        val taskParallism = conf.dag.tasks.get(taskgroupId).get.parallelism

        LOG.info(s"get output taskIds, groupId: $taskgroupId, parallism: $taskParallism")

        0.until(taskParallism).map { taskIndex =>
          TaskId(taskgroupId, taskIndex)
        }
      }.toArray

    } else {
      //outer degree == 0
      this.partitioner = null
      this.outputTaskIds = Array.empty[TaskId]
    }

    this.flowControl = new FlowControl(taskId, outputTaskIds.length)
    this.clockTracker = new ClockTracker(flowControl)

    context.become(waitForStartClock)

    context.parent ! GetStartClock
  }

  private def tryToSyncToClockService() : Unit = {
    if (unackedClockSyncTimestamp == 0) {
      appMaster ! UpdateClock(this.taskId, clockTracker.minClockAtCurrentTask)
      needSyncToClockService = false
      unackedClockSyncTimestamp = System.currentTimeMillis()
    } else {
      val current = System.currentTimeMillis()
      if (current - unackedClockSyncTimestamp > CLOCK_SYNC_TIMEOUT_INTERVAL) {
        appMaster ! UpdateClock(this.taskId, clockTracker.minClockAtCurrentTask)
        needSyncToClockService = false
        unackedClockSyncTimestamp  = System.currentTimeMillis()
      } else {
        needSyncToClockService = true
      }
    }
  }

  private def doHandleMessage() : Unit = {
    var done = false
    while (flowControl.allowSendingMoreMessages() && !done) {
      val msg = queue.poll()
      if (msg != null) {
        msg match {
          case AckRequest(taskId, seq) =>
            transport(Ack(this.taskId, seq), taskId)
            LOG.debug("Sending ack back, taget taskId: " + taskId + ", my task: " + this.taskId + ", my seq: " + seq)
          case m : Message =>
            val updated = clockTracker.onProcess(m)
            if (updated) {
              tryToSyncToClockService()
            }

            onNext(m)
        }
      } else {
        done = true
      }
    }
  }

  def waitForStartClock : Receive = {
    case StartClock(clock) =>
      onStart(new TaskContext(clock))
      context.become(handleMessage)
  }

  def handleMessage : Receive = {
    case ackRequest : AckRequest =>
      //enqueue to handle the ackRequest and send back ack later
      queue.add(ackRequest)
    case ack @ Ack(taskId, seq) =>
      flowControl.receiveAck(taskId, seq)
      val updated = clockTracker.onAck(ack)
      if (updated) {
        tryToSyncToClockService()
      }
      doHandleMessage()
    case msg : Message =>
      if (msg.timestamp != Message.noTimeStamp) {
        latencies.update(System.currentTimeMillis() - msg.timestamp)
      }

      val updatedMessage = clockTracker.onReceive(msg)
      queue.add(updatedMessage)

      doHandleMessage()
    case ClockUpdated(timestamp) =>
      minClock = timestamp
      unackedClockSyncTimestamp = 0
      if (needSyncToClockService) {
        tryToSyncToClockService()
      }
    case RestartTasks(timestamp) =>
      LOG.info(s"Restarting myself $taskId from timestamp $timestamp...")
      throw new RestartException
    case other =>
      LOG.error("Failed! Received unknown message " + "taskId: " + taskId + ", " + other.toString)
  }
}

object TaskActor {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[TaskActor])
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
}