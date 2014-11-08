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
package org.apache.gearpump.streaming

import akka.actor.Actor
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.util.{ActorUtil, Configs}

import scala.collection.mutable.Queue

class TaskLocator(config : Configs) {
  private var userScheduledTask = Map.empty[Class[_ <: Actor], Queue[Locality]]

  initTasks()

  def initTasks() : Unit = {
    val taskLocations : Array[(TaskDescription, Locality)] = ConfigsHelper.loadUserAllocation(ConfigFactory.empty())
    for(taskLocation <- taskLocations){
      val (taskDescription, locality) = taskLocation
      val localityQueue = userScheduledTask.getOrElse(ActorUtil.loadClass(taskDescription.taskClass), Queue.empty[Locality])
      0.until(taskDescription.parallism).foreach(_ => localityQueue.enqueue(locality))
      userScheduledTask += (ActorUtil.loadClass(taskDescription.taskClass) -> localityQueue)
    }
  }

  def locateTask(taskDescription : TaskDescription) : Locality = {
    if(userScheduledTask.contains(ActorUtil.loadClass(taskDescription.taskClass))){
      val localityQueue = userScheduledTask.get(ActorUtil.loadClass(taskDescription.taskClass)).get
      if(localityQueue.size > 0){
        return localityQueue.dequeue()
      }
    }
    NonLocality
  }
}

trait Locality

case class WorkerLocality(workerId : Int) extends Locality

object NonLocality extends Locality

