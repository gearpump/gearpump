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
import org.apache.gearpump.streaming.ExecutorToAppMaster._
import org.apache.gearpump.util.Configs
import org.slf4j.{Logger, LoggerFactory}

abstract class TaskActor(conf : Configs) extends Actor  with ExpressTransport {
  import org.apache.gearpump.streaming.task.TaskActor._

  private val appId = conf.appId
  protected val taskId : TaskId =  conf.taskId

  private val metricName = s"app$appId.task${taskId.groupId}_${taskId.index}"
  private val latencies = Metrics(context.system).histogram(s"$metricName.latency")
  private val throughput = Metrics(context.system).meter(s"$metricName.throughput")

  private[this] val appMaster : ActorRef = conf.appMaster

  private[this] val queue : util.ArrayDeque[Any] = new util.ArrayDeque[Any](INITIAL_WINDOW_SIZE)
  private[this] var partitioner : MergedPartitioner = null

  private[this] var outputTaskIds : Array[TaskId] = null
  private[this] var outputWaterMark : Array[Long] = null
  private[this] var ackRequestWaterMark : Array[Long] = null
  private[this] var ackWaterMark : Array[Long] = null
  private[this] var outputWindow : Long = INITIAL_WINDOW_SIZE

  //report to appMaster with my address
  express.registerLocalActor(TaskId.toLong(taskId), self)

  //We will set this in preStart
  final def receive : Receive = {
    case _ => Unit
  }

  def onStart() : Unit

  def onNext(msg : String) : Unit

  def onStop() : Unit = {}

  def output(msg : String) : Unit = {
    if (null == outputTaskIds) {
      return
    }

    val partitions = partitioner.getPartitions(msg)
    outputWindow -= partitions.length

    var start = 0

    throughput.mark(partitions.length)

    while (start < partitions.length) {
      val partition = partitions(start)

      transport(Message(System.currentTimeMillis(), msg), outputTaskIds(partition))

      outputWaterMark(partition) += 1

      if (outputWaterMark(partition) > ackRequestWaterMark(partition) + FLOW_CONTROL_RATE) {

        transport(AckRequest(taskId, Seq(partition, outputWaterMark(partition))), outputTaskIds(partition))
        ackRequestWaterMark(partition) = outputWaterMark(partition)
      }
      start = start + 1
    }
  }

  final override def postStop : Unit = {

  }

  final override def preStart() : Unit = {

    appMaster ! RegisterTask(taskId, local)

    val graph = conf.dag.graph
    LOG.info(s"TaskInit... taskId: $taskId")
    val outDegree = conf.dag.graph.outDegreeOf(taskId.groupId)

    if (outDegree > 0) {

      val edges = graph.outgoingEdgesOf(taskId.groupId)

      LOG.info(s"task: $taskId out degree is $outDegree, edge length: ${edges.length}")

      this.partitioner = edges.foldLeft(MergedPartitioner.empty) { (mergedPartitioner, nodeEdgeNode) =>
        val (_, partitioner, taskgroupId) = nodeEdgeNode
        val taskParallism = conf.dag.tasks.get(taskgroupId).get.parallism
        mergedPartitioner.add(partitioner, taskParallism)
      }

      LOG.info(s"task: $taskId partitioner: ${partitioner}")

      outputTaskIds = edges.flatMap {nodeEdgeNode =>
        val (_, _, taskgroupId) = nodeEdgeNode
        val taskParallism = conf.dag.tasks.get(taskgroupId).get.parallism

        LOG.info(s"get output taskIds, groupId: $taskgroupId, parallism: $taskParallism")

        0.until(taskParallism).map { taskIndex =>
          TaskId(taskgroupId, taskIndex)
        }
      }.toArray

      this.ackWaterMark = new Array[Long](outputTaskIds.length)
      this.outputWaterMark = new Array[Long](outputTaskIds.length)
      this.ackRequestWaterMark = new Array[Long](outputTaskIds.length)
    } else {
      //outer degree == 0
      this.partitioner = null
      this.outputTaskIds = null
    }
    context.become {
      onStart
      handleMessage
    }
  }

  private def doHandleMessage : Unit = {
    if (outputWindow <= 0) {
      LOG.debug("Touched Flow control, windows size: " + outputWindow)
    }

    var done = false
    while (outputWindow > 0 && !done) {
      val msg = queue.poll()
      if (msg != null) {
        msg match {
          case AckRequest(taskId, seq) =>
            transport(Ack(this.taskId, seq), taskId)
            LOG.debug("Sending ack back, taget taskId: " + taskId + ", my task: " + this.taskId + ", my seq: " + seq)
          case Message(timestamp, msg) =>
            onNext(msg)
          case msg : String =>
            onNext(msg)
        }
      } else {
        done = true
      }
    }
  }

  def handleMessage : Receive = {
    case ackRequest : AckRequest =>
      //enqueue to handle the ackRequest and send back ack later
      queue.add(ackRequest)
    case Ack(taskId, seq) =>
      LOG.debug("get ack from downstream, current: " + this.taskId + "downL: " + taskId + ", seq: " + seq + ", windows: " + outputWindow)
      outputWindow += seq.seq - ackWaterMark(seq.id)
      ackWaterMark(seq.id) = seq.seq
      doHandleMessage
    case msg : Message =>
      queue.add(msg)
      if (msg.timestamp != 0) {
        latencies.update(System.currentTimeMillis() - msg.timestamp)
      }
      doHandleMessage
    case other =>
      LOG.error("Failed! Received unknown message " + "taskId: " + taskId + ", " + other.toString)
  }
}

object TaskActor {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[TaskActor])
  val INITIAL_WINDOW_SIZE = 1024 * 16
  val FLOW_CONTROL_RATE = 100

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

    def getPartitions(msg : String) : Array[Int] = {
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