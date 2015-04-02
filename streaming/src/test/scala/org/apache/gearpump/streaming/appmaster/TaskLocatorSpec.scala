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

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.{ClusterConfig, UserConfig}
import org.apache.gearpump.streaming.ProcessorDescription
import org.apache.gearpump.streaming.appmaster.TaskLocator.{NonLocality, WorkerLocality}
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}
import org.apache.gearpump.util.Constants._
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable.ArrayBuffer

class TaskLocatorSpec extends WordSpec with Matchers {
  val resource = getClass.getClassLoader.getResource("tasklocation.conf").getPath
  System.setProperty(GEARPUMP_CUSTOM_CONFIG_FILE, resource)
  val taskDescription1 = ProcessorDescription("org.apache.gearpump.streaming.appmaster.TestTask1", 4)
  val taskDescription2 = ProcessorDescription("org.apache.gearpump.streaming.appmaster.TestTask2", 2)
  val taskDescription3 = ProcessorDescription("org.apache.gearpump.streaming.appmaster.TestTask3", 2)

  val config = ClusterConfig.load.application

  "TaskLocator" should {
    "locate task properly according user's configuration" in {
      val taskLocator = new TaskLocator(config)
      assert(taskLocator.locateTask(taskDescription2) == WorkerLocality(1))
      assert(taskLocator.locateTask(taskDescription2) == WorkerLocality(1))
      assert(taskLocator.locateTask(taskDescription2) == NonLocality)
      val localities = ArrayBuffer[WorkerLocality]()
      for (i <- 0 until 4) {
        localities.append(taskLocator.locateTask(taskDescription1).asInstanceOf[WorkerLocality])
      }
      localities.sortBy(_.workerId)
      assert(localities(0) == WorkerLocality(2))
      assert(localities(1) == WorkerLocality(2))
      assert(localities(2) == WorkerLocality(1))
      assert(localities(3) == WorkerLocality(1))
      assert(taskLocator.locateTask(taskDescription1) == NonLocality)
      assert(taskLocator.locateTask(taskDescription3) == NonLocality)
    }
  }
}

class TestTask1(taskContext : TaskContext, userConf : UserConfig)
    extends Task(taskContext, userConf) {
  override def onStart(startTime: StartTime): Unit = ???
  override def onNext(msg: Message): Unit = ???
}

class TestTask2(taskContext : TaskContext, userConf : UserConfig)
    extends Task(taskContext, userConf) {
  override def onStart(startTime: StartTime): Unit = ???
  override def onNext(msg: Message): Unit = ???
}

class TestTask3(taskContext : TaskContext, userConf : UserConfig)
    extends Task(taskContext, userConf) {
  override def onStart(startTime: StartTime): Unit = ???
  override def onNext(msg: Message): Unit = ???
}