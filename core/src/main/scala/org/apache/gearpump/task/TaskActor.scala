package org.apache.gearpump.task
/**
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

import java.util
import java.util.concurrent.TimeUnit

import akka.actor.{Stash, Props, ActorRef, Actor}
import com.google.common.collect.MinMaxPriorityQueue
import org.apache.gearpump.task.TaskActor.AskForTaskLocations
import org.apache.gearpump.{StageParallism, Partitioner, TaskLocation, GetTaskLocation}
import org.slf4j.{LoggerFactory, Logger}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, promise, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success,Failure}

case class TaskInit(taskId : TaskId, master : ActorRef, outputs : StageParallism, conf : Map[String, Any], partitioner : Partitioner)

abstract class TaskActor extends Actor  with Stash {
  import TaskActor._

  private var taskId : TaskId = null
  private var outputTaskLocations : Array[ActorRef] = null
  private val queue : util.ArrayDeque[Any] = new util.ArrayDeque[Any](INITIAL_WINDOW_SIZE)
  private var conf : Map[String, Any] = null
  private var partitioner : Partitioner = null
  private var inputTaskLocations = Map[TaskId, ActorRef]();
  private var outputs : StageParallism = null
  private var outputStatus : Array[Long] = null
  private var ackStatus : Array[Long] = null
  private var outputWindow : Long = INITIAL_WINDOW_SIZE

  final def receive : Receive = init

  final override def preStart() : Unit = {}

  def onStart() : Unit

  def onNext(msg : String) : Unit

  def onStop() : Unit = {}

  def output(msg : String) : Unit = {

    if (outputs.parallism == 0) {
      return
    }

    outputWindow -= 1
    val partition = partitioner.getPartition(msg, outputs.parallism)
    outputStatus(partition) += 1
    outputTaskLocations(partition).tell(msg, ActorRef.noSender)
    if (outputStatus(partition) % FLOW_CONTROL_RATE == 0) {
      outputTaskLocations(partition).tell(Send(taskId, outputStatus(partition)), ActorRef.noSender)
    }
  }

  def init : Receive = {
    case TaskInit(taskId, master, outputs, conf, partitioner)  => {
      LOG.info("TaskInit... taskId: " + taskId + ", ouput: " + outputs)

      this.taskId = taskId
      this.conf = conf
      this.partitioner = partitioner
      this.outputs = outputs
      this.ackStatus = new Array[Long](outputs.parallism)
      this.outputStatus = new Array[Long](outputs.parallism)
      this.outputTaskLocations = new  Array[ActorRef](outputs.parallism)

      if (outputs.parallism > 0) {
        LOG.info("becoming wait for output task locations...." + taskId)
        context.actorOf(Props(new AskForTaskLocations(master, outputs, self)))
        context.become(waitForOutputTaskLocations)
      } else {
        LOG.info("becoming handleMessage...." + taskId)
        context.become {
          onStart
          handleMessage
        }
      }
    }
  }

  def waitForOutputTaskLocations : Receive = {
    case TaskLocations(locations) =>

      for ((k, v) <- locations) {
        val taskIndex = k.index
        outputTaskLocations(taskIndex) = v
      }

      //Send identiy of self to downstream
      outputTaskLocations.foreach(_ ! Identity(taskId))
      context.become {
        onStart
        handleMessage
      }
      unstashAll()
    case msg : Any =>
      stash()
  }

  private def doHandleMessage : Unit = {
    var msg = queue.poll()

    if (outputWindow <= 0) {
      LOG.info("Touched Flow control, windows size: " + outputWindow)
    }


    while (outputWindow > 0 && null != msg) {
      msg match {
        case Send(taskId, seq) =>
          inputTaskLocations(taskId).tell(Ack(this.taskId, seq), ActorRef.noSender)
          LOG.info("Sending ack back, taget taskId: " + taskId + ", my task: " + this.taskId + ", my seq: " + seq)
        case _ =>
          onNext(msg.asInstanceOf[String])
      }
      msg = queue.poll()
    }
  }

  def handleMessage : Receive = {
    case Identity(taskId) =>
      LOG.info("get identity from upstream: " + taskId)
      val upStream = sender
      inputTaskLocations = inputTaskLocations + (taskId->upStream)
    case send : Send =>
      queue.add(send)
    case Ack(taskId, seq) =>
      LOG.info("get ack from downstream, current: " + this.taskId + "downL: " + taskId + ", seq: " + seq + ", windows: " + outputWindow)
      outputWindow += seq - ackStatus(taskId.index)
      ackStatus(taskId.index) = seq
      doHandleMessage
    case msg : String =>
      queue.add(msg)
      doHandleMessage
    case other =>
      LOG.error("Failed! Received unknown message " + "taskId: " + taskId + ", " + other.toString)
  }
}

object TaskActor {
  private val LOG: Logger = LoggerFactory.getLogger(TaskActor.getClass)
  val INITIAL_WINDOW_SIZE = 1024 * 16
  val FLOW_CONTROL_RATE = 100

  class AskForTaskLocations(master : ActorRef, nextStage : StageParallism, parent : ActorRef) extends Actor {

    LOG.info("Creating Ask task for ... " + parent.path)

    override def preStart() : Unit = {
      0.until(nextStage.parallism).map((taskIndex) => {
        val taskId = TaskId(nextStage.stageId, taskIndex)
        master ! GetTaskLocation(taskId)
      })
    }

    def receive = waitForTaskLocation(Map[TaskId, ActorRef]())

    def waitForTaskLocation(taskLocationMap : Map[TaskId, ActorRef]) : Receive = {
      case TaskLocation(taskId, task) => {
        LOG.info("Get task location, taskId: " + taskId)

        val newLocationMap = taskLocationMap.+((taskId, task))

        LOG.info("new Location Map: " + newLocationMap.toString())
        LOG.info("output Task size: " + nextStage.parallism + ", location map size: " + newLocationMap.size)

        if (newLocationMap.size == nextStage.parallism) {

          parent ! TaskLocations(newLocationMap)
          LOG.info("Sending task location to " + parent.path)
          context.stop(self)
        } else {
          waitForTaskLocation(newLocationMap)
        }
      }
    }
  }
}