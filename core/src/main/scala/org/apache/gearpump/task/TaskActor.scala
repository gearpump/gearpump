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

import akka.actor.{Props, ActorRef, Actor}
import org.apache.gearpump.task.TaskActor.AskForTaskLocations
import org.apache.gearpump.{Partitioner, TaskLocation, GetTaskLocation}
import org.slf4j.{LoggerFactory, Logger}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

case class TaskInit(taskId : Int, master : ActorRef, outputs : Range, conf : Map[String, Any], partitioner : Partitioner)

abstract class TaskActor extends Actor {
  import TaskActor._

  private var outputTaskCount = 0
  private var taskId = -1
  private var outputTaskLocations : Array[ActorRef] = null
  private val queue : util.ArrayDeque[Any] = new util.ArrayDeque[Any](INITIAL_QUEUE_SIZE)
  private var conf : Map[String, Any] = null
  private var partitioner : Partitioner = null
  private var inputTaskLocations = Map[Int, ActorRef]();
  private var outputs : Range = null
  private var outputStatus : Array[Int] = null

  final def receive : Receive = init

  def onNext(msg : String) : Unit = {}

  def onStop() : Unit = {}

  def output(msg : String) : Unit = {
    val partition = partitioner.getPartition(msg, outputTaskCount)
    outputTaskLocations(partition) ! msg
  }

  def init : Receive = {
    case TaskInit(taskId, master, outputs, conf, partitioner)  => {
      val locations = Promise[Array[ActorRef]]()
      this.taskId = taskId
      this.conf = conf
      this.partitioner = partitioner
      this.outputs = outputs
      this.outputStatus = new Array[Int](outputs.size)
      context.actorOf(Props(new AskForTaskLocations(master, outputs, locations)))
      outputTaskLocations = Await.result[Array[ActorRef]](locations.future, Duration(30, TimeUnit.SECONDS))
      outputTaskCount = outputTaskLocations.length

      //Send identiy of self to downstream
      outputTaskLocations.foreach(_ ! Identity(taskId))
      context.become(handleMessage)
    }
  }

  case class SenderInfo(sender : ActorRef, seq : Int)

  def handleMessage : Receive = {
    case Identity(taskId) =>
      val upStream = sender
      inputTaskLocations = inputTaskLocations + (taskId->upStream)
    case Ack(taskId, seq) =>
      val offset = taskId - outputs.start
      outputStatus(offset) = seq

    case msg : String =>
      //TODO: push the data to queue

      onNext(msg)
    case other =>
      LOG.error("Failed! Received unknown message " + other.toString)
  }
}

object TaskActor {
  private val LOG: Logger = LoggerFactory.getLogger(TaskActor.getClass)
  private val INITIAL_QUEUE_SIZE = 1024

  class AskForTaskLocations(master : ActorRef, outputTasks : Range, locations : Promise[Array[ActorRef]]) extends Actor {
    override def preStart() : Unit = {
      outputTasks.foreach {
        master ! GetTaskLocation(_)
      }
    }

    def receive = waitForTaskLocation(Map[Int, ActorRef]())

    def waitForTaskLocation(taskLocationMap : Map[Int, ActorRef]) : Receive = {
      case TaskLocation(taskId, task) => {
        val newLocationMap = taskLocationMap.+((taskId, task))
        if (newLocationMap.size == outputTasks.size) {
          locations.success(newLocationMap.toArray.sortBy(_._1).map(_._2))
          context.stop(self)
        } else {
          waitForTaskLocation(newLocationMap)
        }
      }
    }
  }
}