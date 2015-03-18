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

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.TaskDescription
import org.apache.gearpump.streaming.appmaster.ScheduleUsingUserConfig.{WorkerLocality, NonLocality, Locality}
import org.apache.gearpump.streaming.task.{TaskUtil, Task}
import org.apache.gearpump.util.Util

import scala.collection.mutable

trait TaskSchedulePolicy {
  def scheduleTask(taskDescription: TaskDescription) : Locality
}

class NoneSchedulePolicy extends TaskSchedulePolicy {
  override def scheduleTask(taskDescription: TaskDescription): Locality = NonLocality
}

class ScheduleUsingDataDetector(systemConfig: Config, userConfig: UserConfig, workers: Map[String, Set[Int]], system: ActorSystem) extends TaskSchedulePolicy {
  private var userScheduledTask = Map.empty[Class[_ <: Task], mutable.Queue[Locality]]
  private var dataConnectors = Map.empty[String, DataDetector]

  init()

  def init(): Unit = {
    initDataDetectors(systemConfig)
    dataConnectors.foreach { kv =>
      val (task, dataDetector) = kv
      val taskClass = TaskUtil.loadClass(task)
      val localities = userScheduledTask.getOrElse(taskClass, mutable.Queue.empty[Locality])
      val tasksOnHost = dataDetector.getTaskNumOnHosts()
      tasksOnHost.foreach { ipAndTaskNum =>
        val (ip, taskNum) = ipAndTaskNum
        0.until(taskNum).foreach(_ => localities.enqueue(translateIpToLocality(ip)))
      }
      userScheduledTask += taskClass -> localities
    }
  }

  override def scheduleTask(taskDescription: TaskDescription): Locality = {
    if(userScheduledTask.contains(TaskUtil.loadClass(taskDescription.taskClass))){
      val localityQueue = userScheduledTask.get(TaskUtil.loadClass(taskDescription.taskClass)).get
      if(localityQueue.size > 0){
        return localityQueue.dequeue()
      }
    }
    NonLocality
  }

  /*
  gearpump {
    application {
      data {
        detector {
          task1 = dataconnector1
          task2 = dataconnector2
        }
      }
    }
  }
  */
  def initDataDetectors(config: Config): Unit = {
    if(config.hasPath(ScheduleUsingDataDetector.POLICY)) {
      val taskLocatorMap = Util.configToMap(config, ScheduleUsingDataDetector.POLICY)
      taskLocatorMap.foreach { kv =>
        val (task, dataDetectorClass) = kv
        val dataDetector = Class.forName(dataDetectorClass).newInstance().asInstanceOf[DataDetector]
        dataDetector.init(userConfig)(system)
        dataConnectors += task -> dataDetector
      }
    }
  }

  private def translateIpToLocality(ip: String): Locality = {
    if(workers.contains(ip)){
      WorkerLocality(workers.get(ip).get.head)
    } else {
      NonLocality
    }
  }
}

object ScheduleUsingDataDetector {
  final val POLICY = "gearpump.application.data.detector"
}
