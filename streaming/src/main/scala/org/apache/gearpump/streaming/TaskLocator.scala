package org.apache.gearpump.streaming

import akka.actor.Actor
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.util.Configs

import scala.collection.mutable.Queue

class TaskLocator(config : Configs) {
  private var userScheduledTask = Map.empty[Class[_ <: Actor], Queue[Locality]]

  initTasks()

  def initTasks() : Unit = {
    val taskLocations : Array[(TaskDescription, Locality)] = ConfigsHelper.loadUserAllocation(ConfigFactory.empty)
    for(taskLocation <- taskLocations){
      val (taskDescription, locality) = taskLocation
      val localityQueue = userScheduledTask.getOrElse(taskDescription.taskClass, Queue.empty[Locality])
      0.until(taskDescription.parallism).foreach(_ => localityQueue.enqueue(locality))
      userScheduledTask += (taskDescription.taskClass -> localityQueue)
    }
  }

  def locateTask(taskDescription : TaskDescription) : Locality = {
    if(userScheduledTask.contains(taskDescription.taskClass)){
      val localityQueue = userScheduledTask.get(taskDescription.taskClass).get
      if(localityQueue.size > 0){
        localityQueue.dequeue()
      }
    }
    NonLocality
  }
}

trait Locality

case class WorkerLocality(workerId : Int) extends Locality

object NonLocality extends Locality

