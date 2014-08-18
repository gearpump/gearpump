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

package org.apache.gearpump.task

import java.util
import java.util.concurrent.{TimeUnit, TimeoutException}

import akka.actor._
import com.codahale.metrics.{Slf4jReporter, ConsoleReporter, JmxReporter, MetricRegistry}
import org.apache.gearpump.transport.ExpressAddress
import org.apache.gearpump.{Partitioner, StageParallism}
import org.apache.gears.cluster.AppMasterToExecutor._
import org.apache.gears.cluster.Configs
import org.apache.gears.cluster.ExecutorToAppMaster._
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

abstract class TaskActor(conf : Configs) extends Actor  with Stash with ExpressTransport {
  import org.apache.gearpump.task.TaskActor._

  protected val taskId : TaskId =  conf.taskId

  private val metrics = new MetricRegistry()
  private val latencies = metrics.histogram(s"task[$taskId] latency: ")
  private val windowSize = metrics.histogram(s"task[$taskId] window size:")

  val reporter = Slf4jReporter.forRegistry(metrics)
                  .convertRatesTo(TimeUnit.SECONDS)
                  .convertDurationsTo(TimeUnit.MILLISECONDS)
                  .build()

  private[this] val appMaster : ActorRef = conf.appMaster

  private[this] val queue : util.ArrayDeque[Any] = new util.ArrayDeque[Any](INITIAL_WINDOW_SIZE)
  private[this] var partitioner : MergedPartitioner = null
  private[this] var inputTaskLocations = Map[TaskId, ExpressAddress]()

  private[this] var outputTaskIds : Array[TaskId] = null
  private[this] var outputTaskLocations : Array[ExpressAddress] = null
  private[this] var outputWaterMark : Array[Long] = null
  private[this] var ackRequestWaterMark : Array[Long] = null
  private[this] var ackWaterMark : Array[Long] = null
  private[this] var outputWindow : Long = INITIAL_WINDOW_SIZE


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
    while (start < partitions.length) {
      val partition = partitions(start)

      transport(Message(System.currentTimeMillis(), msg), outputTaskLocations(partition))

      outputWaterMark(partition) += 1

      if (outputWaterMark(partition) > ackRequestWaterMark(partition) + FLOW_CONTROL_RATE) {

        transport(AckRequest(taskId, outputWaterMark(partition)), outputTaskLocations(partition))
        ackRequestWaterMark(partition) = outputWaterMark(partition)
      }
      start = start + 1
    }
  }

  final override def postStop : Unit = {
    reporter.stop()
  }

  final override def preStart() : Unit = {

    appMaster ! TaskLaunched(taskId, local)

    val graph = conf.dag.graph
    LOG.info(s"TaskInit... taskId: $taskId")
    val outDegree = conf.dag.graph.outDegreeOf(taskId.groupId)

    reporter.start(10, TimeUnit.SECONDS)

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
      this.outputTaskLocations = new  Array[ExpressAddress](outputTaskIds.length)

      LOG.info("becoming wait for output task locations...." + taskId)

      context.actorOf(Props(new AskForTaskLocations(this.taskId, appMaster, outputTaskIds, self)))
      context.become(waitForOutputTaskLocations)

    } else {
      //outer degree == 0
      this.partitioner = null
      this.outputTaskIds = null

      context.become {
        onStart
        handleMessage
      }
    }
  }

  def waitForOutputTaskLocations : Receive = {
    case _ : Failure[TaskLocations] => {
      LOG.error(s"failed to get all task locations, stop current task $taskId")
      context.stop(self)
    }
    case taskLocation : Success[TaskLocations] => {
      val TaskLocations(locations) = taskLocation.get
      for ((k, v) <- locations) {
        val taskIndex = k.index
        outputTaskLocations(taskIndex) = v
      }

      //Send identiy of self to downstream
      outputTaskLocations.foreach { remote =>
        transport(Identity(taskId, local), remote)
      }
      context.become {
        onStart
        handleMessage
      }
      unstashAll()
    }
    case msg: Any =>
      stash()
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

            transport(Ack(this.taskId, seq), inputTaskLocations(taskId))
            LOG.debug("Sending ack back, taget taskId: " + taskId + ", my task: " + this.taskId + ", my seq: " + seq)
          case Message(timestamp, msg) =>
            windowSize.update(outputWindow)
            onNext(msg)
          case msg : String =>
            onNext(msg)
            windowSize.update(outputWindow)
        }
      } else {
        done = true
      }
    }
  }

  def handleMessage : Receive = {
    case Identity(taskId, upStream) =>
      LOG.info("get identity from upstream: " + taskId)
      inputTaskLocations = inputTaskLocations + (taskId->upStream)
    case ackRequest : AckRequest =>
      //enqueue to handle the ackRequest and send back ack later
      queue.add(ackRequest)
    case Ack(taskId, seq) =>
      LOG.debug("get ack from downstream, current: " + this.taskId + "downL: " + taskId + ", seq: " + seq + ", windows: " + outputWindow)
      outputWindow += seq - ackWaterMark(taskId.index)
      ackWaterMark(taskId.index) = seq
      doHandleMessage
    case msg : Message =>
      queue.add(msg)
      latencies.update(System.currentTimeMillis() - msg.timestamp)
      doHandleMessage
    case other =>
      LOG.error("Failed! Received unknown message " + "taskId: " + taskId + ", " + other.toString)
  }
}

object TaskActor {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[TaskActor])
  val INITIAL_WINDOW_SIZE = 1024 * 16
  val FLOW_CONTROL_RATE = 100

  class AskForTaskLocations(taskId : TaskId, master : ActorRef, taskIds : Array[TaskId], parent : ActorRef) extends Actor {

    context.setReceiveTimeout(FiniteDuration(10, TimeUnit.SECONDS))

    LOG.info(s"[${this.taskId}] Ask task location for ${taskIds.mkString}")

    override def preStart() : Unit = {
      taskIds.foreach{master ! GetTaskLocation(_)}
    }

    def receive = waitForTaskLocation(Map[TaskId, ExpressAddress]())

    def waitForTaskLocation(taskLocationMap : Map[TaskId, ExpressAddress]) : Receive = {
      case TaskLocation(taskId, task) => {
        LOG.info(s"[${this.taskId}] We received a task location, taskId: " + taskId)

        val newLocationMap = taskLocationMap.+((taskId, task))

        LOG.debug(s"[${this.taskId}] new Location Map: " + newLocationMap.toString())
        LOG.info(s"[${this.taskId}] output Task size: " + taskIds.length + ", location map size: " + newLocationMap.size)

        if (newLocationMap.size == taskIds.length) {
          parent ! Success(TaskLocations(newLocationMap))
          LOG.info(s"[${this.taskId}] We have collected all downstream task locations, send them to ${parent.path.name} ...")
          context.stop(self)
        } else {
          context.become(waitForTaskLocation(newLocationMap))
        }
      }
      case ReceiveTimeout =>
        LOG.error("AskForTaskLocations timeout! We have not gathered enough task location to continue...")
        parent ! Failure(new TimeoutException(s"Failed to get all task locations, we want we already get ${taskIds.length}, but can only get " +
          s"${taskLocationMap.keySet.size}, details: $taskLocationMap"))
        context.stop(self)
    }
  }

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