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

import akka.actor.{ActorRef, Actor, Props, ActorSystem}
import akka.testkit.TestProbe
import org.apache.gearpump.cluster.TestUtil
import org.apache.gearpump.cluster.scheduler.{ResourceRequest, Resource}
import org.apache.gearpump.partitioner.{Partitioner, HashPartitioner}
import org.apache.gearpump.streaming.AppMasterToExecutor.LaunchTask
import org.apache.gearpump.streaming.Executor.RestartExecutor
import org.apache.gearpump.streaming.ExecutorToAppMaster.RegisterTask
import org.apache.gearpump.streaming.appmaster.AppMaster.AllocateResourceTimeOut
import org.apache.gearpump.streaming.appmaster.ExecutorManager._
import org.apache.gearpump.streaming.appmaster.TaskManager.MessageLoss
import org.apache.gearpump.streaming.appmaster.TaskManagerSpec.{Env, Task1, Task2}
import org.apache.gearpump.streaming.appmaster.TaskSchedulerImpl.TaskLaunchData
import org.apache.gearpump.streaming.task.{TaskLocations, GetLatestMinClock, UpdateClock, TaskId}
import org.apache.gearpump.streaming.{DAG, TaskDescription}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Graph
import org.apache.gearpump.util.Graph._
import org.mockito.Mockito
import org.scalatest.{FlatSpec, BeforeAndAfterEach, Matchers, WordSpec}
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

class TaskManagerSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  implicit var system: ActorSystem = null

  val task1Class = classOf[Task1].getName
  val task2Class = classOf[Task2].getName

  val task1 = TaskDescription(task1Class, 1)
  val task2 = TaskDescription(task2Class, 1)
  val partitioner = new HashPartitioner()

  val dag: DAG = Graph[TaskDescription, Partitioner](task1 ~ partitioner ~> task2)

  val task1LaunchData = TaskLaunchData(TaskId(0, 0), TaskDescription(task1Class,
    parallelism = 1), dag)
  val task2LaunchData = TaskLaunchData(TaskId(1, 0), TaskDescription(task2Class,
    parallelism = 1), dag)

  val appId = 0

  val resource = Resource(2)
  val workerId = 0
  val executorId = 0

  override def beforeEach(): Unit = {
    system = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
  }

  override def afterEach(): Unit = {
    system.shutdown()
  }

  it should "start the bootup the executor and tasks, and serve clock query correctly" in {
    val env = bootUp
    import env._

    //task heartbeat its minclock to taskmanager
    val updateClock = UpdateClock(TaskId(0, 0), System.currentTimeMillis())
    taskManager ! updateClock

    //taskmanager will forward to clockservice
    clockService.expectMsg(updateClock)

    taskManager ! GetLatestMinClock
    clockService.expectMsg(GetLatestMinClock)
  }

  it should "recover by requesting new executors when executor stopped unexpectedly" in {
    val env = bootUp
    import env._

    val resourceRequest = Array(ResourceRequest(resource, workerId))
    when(scheduler.executorFailed(executorId)).thenReturn(resourceRequest)


    taskManager ! ExecutorStopped(executorId)

    // when one executor stop, it will also trigger the recovery by restart
    // existing executors
    executorManager.expectMsg(BroadCast(RestartExecutor))

    import scala.concurrent.duration._

    // ask for new executors
    val returned = executorManager.expectMsg(StartExecutors(resourceRequest))
    executorManager.reply(StartExecutorsTimeOut)

    // TaskManager cannot handle the TimeOut error itself, escalate to appmaster.
    appMaster.expectMsg(AllocateResourceTimeOut)
  }

  it should "recover by restarting existing executors when message loss happen" in {
    val env = bootUp
    import env._

    taskManager ! MessageLoss

    // Restart the executors so that we can replay from minClock
    executorManager.expectMsg(BroadCast(RestartExecutor))
  }

  private def bootUp: Env = {
    val executorManager = TestProbe()
    val clockService = TestProbe()
    val appMaster = TestProbe()
    val executor = TestProbe()

    val scheduler = mock(classOf[TaskScheduler])
    val taskManager = system.actorOf(
      Props(new TaskManager(appId, dag, scheduler, executorManager.ref, clockService.ref, appMaster.ref, "appName")))

    executorManager.expectMsgType[SetTaskManager]
    executorManager.expectMsgType[StartExecutors]

    when(scheduler.resourceAllocated(workerId, executorId))
      .thenReturn(
        //first call
        Some(task1LaunchData),
        //second call
        Some(task2LaunchData))

    executorManager.reply(ExecutorStarted(executor.ref, executorId, resource, workerId))

    executor.expectMsgType[LaunchTask]

    //scheduler.resourceAllocated will be called here to ask for task to be started

    // register task(0,0)
    executor.reply(RegisterTask(TaskId(0, 0), executorId, HostPort("127.0.0.1:3000")))

    // taskmanager should return the latest clock to task(0,0)
    clockService.expectMsg(GetLatestMinClock)

    executor.expectMsgType[LaunchTask]

    //register task(1,0)
    executor.reply(RegisterTask(TaskId(1, 0), executorId, HostPort("127.0.0.1:3000")))

    // taskmanager should return the latest clock to task(1,0)
    clockService.expectMsg(GetLatestMinClock)

    //broad cast task locations to all tasks, now the app is ready
    import scala.concurrent.duration._
    assert(executorManager.expectMsgPF(5 seconds) {
      case BroadCast(taskLocation) => taskLocation.isInstanceOf[TaskLocations]
    })

    Env(executorManager, clockService, appMaster, executor, taskManager, scheduler)
  }
}

object TaskManagerSpec {
  case class Env(
    executorManager: TestProbe,
    clockService: TestProbe,
    appMaster: TestProbe,
    executor: TestProbe,
    taskManager: ActorRef,
    scheduler: TaskScheduler)

  class Task1 extends Actor {
    def receive: Receive = null
  }

  class Task2 extends Actor {
    def receive: Receive = null
  }
}
