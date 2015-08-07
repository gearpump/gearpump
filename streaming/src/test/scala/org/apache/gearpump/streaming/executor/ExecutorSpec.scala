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
package org.apache.gearpump.streaming.executor

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestProbe
import org.apache.gearpump.cluster.appmaster.WorkerInfo
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.cluster.{ExecutorContext, TestUtil, UserConfig}
import org.apache.gearpump.streaming.AppMasterToExecutor.{ChangeTask, ChangeTasks, LaunchTasks, TasksChanged, TasksLaunched}
import org.apache.gearpump.streaming.executor.TaskLauncherSpec.MockTask
import org.apache.gearpump.streaming.task.{Subscriber, TaskId}
import org.apache.gearpump.streaming.{LifeTime, ProcessorDescription}
import org.mockito.Matchers._
import org.mockito.Mockito.{times, _}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.language.postfixOps


class ExecutorSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val appId = 0
  val executorId = 0
  val workerId = 0
  var appMaster: TestProbe = null
  implicit var system: ActorSystem = null
  val userConf = UserConfig.empty

  override def beforeAll(): Unit = {
    system = ActorSystem("TaskLauncherSpec", TestUtil.DEFAULT_CONFIG)
    appMaster = TestProbe()
  }

  override def afterAll(): Unit = {
    system.shutdown()
  }

  it should "call launcher to launch task" in {
    val worker = TestProbe()
    val workerInfo = WorkerInfo(workerId, worker.ref)
    val executorContext = ExecutorContext(executorId, workerInfo, appId, "app", appMaster.ref, Resource(2))
    val taskLauncher = mock(classOf[ITaskLauncher])
    val executor = system.actorOf(Props(new Executor(executorContext, userConf, taskLauncher)))
    val processor = ProcessorDescription(id = 0, taskClass = classOf[MockTask].getName, parallelism = 2)
    val taskIds = List(TaskId(0, 0), TaskId(0, 1))
    val launchTasks = LaunchTasks(taskIds, dagVersion = 0,  processor, List.empty[Subscriber])

    val task = TestProbe()
    when(taskLauncher.launch(any(), any(), any(), any())).thenReturn(taskIds.map((_, task.ref)).toMap)

    val client = TestProbe()
    client.send(executor, launchTasks)
    client.expectMsg(TasksLaunched)

    verify(taskLauncher, times(1)).launch(any(), any(), any(), any())

    val changeTasks = ChangeTasks(taskIds, dagVersion = 1, life = LifeTime(0, Long.MaxValue), List.empty[Subscriber])

    client.send(executor, changeTasks)
    client.expectMsg(TasksChanged)

    task.expectMsgType[ChangeTask]
    task.expectMsgType[ChangeTask]
  }
}