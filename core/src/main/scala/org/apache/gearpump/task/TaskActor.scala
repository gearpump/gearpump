package org.apache.gearpump.task

import java.util.concurrent.TimeUnit

import akka.actor.{Props, ActorRef, Actor}
import org.apache.gearpump.task.TaskActor.AskForTaskLocations
import org.apache.gearpump.{Partitioner, TaskLocation, GetTaskLocation}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

/**
 * Created by xzhong10 on 2014/7/23.
 */

case class TaskInit(master : ActorRef, outputs : Array[Int])

abstract class TaskActor(val conf : Map[String, Any], partitioner : Partitioner) extends Actor {
  private var outputTaskCount = 0
  private var outputTaskLocations : Array[ActorRef] = null

  final def receive : Receive = init

  def onNext(msg : String) : Unit = {}

  def onStop() : Unit = {}

  def output(msg : String) : Unit = {
    val partition = partitioner.getPartition(msg, outputTaskCount)
    outputTaskLocations(partition) ! msg
  }

  def init : Receive = {
    case TaskInit(master, outputs)  => {
      val locations = Promise[Array[ActorRef]]()
      context.actorOf(Props(new AskForTaskLocations(master, outputs, locations)))
      outputTaskLocations = Await.result[Array[ActorRef]](locations.future, Duration(30, TimeUnit.SECONDS))
      outputTaskCount = outputTaskLocations.length
      context.become(handleMessage)
    }
  }

  def handleMessage : Receive = {
    case _ => Unit
  }
}

object TaskActor {
  class AskForTaskLocations(master : ActorRef, outputTasks : Array[Int], locations : Promise[Array[ActorRef]]) extends Actor {
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