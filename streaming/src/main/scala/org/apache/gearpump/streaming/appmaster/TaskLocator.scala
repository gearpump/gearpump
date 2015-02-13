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
package org.apache.gearpump.streaming.appmaster

import java.util

import akka.actor.Actor
import com.typesafe.config.Config
import org.apache.gearpump.cluster.ClusterConfig
import org.apache.gearpump.streaming.TaskDescription
import org.apache.gearpump.streaming.appmaster.TaskLocator.{WorkerLocality, NonLocality, Locality}
import org.apache.gearpump.streaming.task.{Task, TaskUtil}
import org.apache.gearpump.util.{ActorUtil, Constants}

import scala.collection.mutable

class TaskLocator(config: Config) {
  private var userScheduledTask = Map.empty[Class[_ <: Task], mutable.Queue[Locality]]

  initTasks()

  def initTasks() : Unit = {
    val taskLocations : Array[(TaskDescription, Locality)] = loadUserAllocation(config)
    for(taskLocation <- taskLocations){
      val (taskDescription, locality) = taskLocation
      val localityQueue = userScheduledTask.getOrElse(TaskUtil.loadClass(taskDescription.taskClass), mutable.Queue.empty[Locality])
      0.until(taskDescription.parallelism).foreach(_ => localityQueue.enqueue(locality))
      userScheduledTask += (TaskUtil.loadClass(taskDescription.taskClass) -> localityQueue)
    }
  }

  def locateTask(taskDescription : TaskDescription) : Locality = {
    if(userScheduledTask.contains(TaskUtil.loadClass(taskDescription.taskClass))){
      val localityQueue = userScheduledTask.get(TaskUtil.loadClass(taskDescription.taskClass)).get
      if(localityQueue.size > 0){
        return localityQueue.dequeue()
      }
    }
    NonLocality
  }

  /*
The task resource requests format:
gearpump {
  scheduling {
    requests {
      worker1 {
        "task1" = 2   //parallelism
        "task2" = 4
      }
      worker2 {
        "task1" = 2
      }
    }
  }
}
 */
  def loadUserAllocation(config: Config) : Array[(TaskDescription, Locality)] ={
    import scala.collection.JavaConverters._
    var result = new Array[(TaskDescription, Locality)](0)
    if(!config.hasPath(Constants.GEARPUMP_SCHEDULING_REQUEST))
      return result
    val requests = config.getObject(Constants.GEARPUMP_SCHEDULING_REQUEST)
    for(workerId <- requests.keySet().asScala.toSet[String]){
      val taskDescriptions = requests.get(workerId).unwrapped().asInstanceOf[util.HashMap[String, Int]].asScala
      for(taskDescription <- taskDescriptions){
        val (taskClass, parallism) = taskDescription
        result = result :+ (TaskDescription(taskClass, parallism), WorkerLocality(workerId.toInt))
      }
    }
    result
  }
}

object TaskLocator {

  trait Locality

  case class WorkerLocality(workerId: Int) extends Locality

  object NonLocality extends Locality

}

