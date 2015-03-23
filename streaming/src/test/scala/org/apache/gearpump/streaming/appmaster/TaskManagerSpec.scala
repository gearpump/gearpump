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

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.TestProbe
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.scheduler.{Resource, ResourceRequest}
import org.apache.gearpump.cluster.{TestUtil, UserConfig}
import org.apache.gearpump.partitioner.{HashPartitioner, Partitioner}
import org.apache.gearpump.streaming.AppMasterToExecutor.{StartClock, LaunchTask}
import org.apache.gearpump.streaming.executor.Executor
import Executor.RestartExecutor
import org.apache.gearpump.streaming.ExecutorToAppMaster.RegisterTask
import org.apache.gearpump.streaming.appmaster.AppMaster.AllocateResourceTimeOut
import org.apache.gearpump.streaming.appmaster.ExecutorManager._
import org.apache.gearpump.streaming.appmaster.TaskManager.MessageLoss
import org.apache.gearpump.streaming.appmaster.TaskManagerSpec.{Env, Task1, Task2}
import org.apache.gearpump.streaming.appmaster.TaskSchedulerImpl.TaskLaunchData
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.streaming.{DAG, ProcessorDescription}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Graph
import org.apache.gearpump.util.Graph._
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class TaskManagerSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  implicit var system: ActorSystem = null

  val task1Class = classOf[Task1].getName
  val task2Class = classOf[Task2].getName

  val task1 = ProcessorDescription(task1Class, 1)
  val task2 = ProcessorDescription(task2Class, 1)
  val partitioner = new HashPartitioner()

  val dag: DAG = Graph[ProcessorDescription, Partitioner](task1 ~ partitioner ~> task2)

  val task1LaunchData = TaskLaunchData(TaskId(0, 0), ProcessorDescription(task1Class,
    parallelism = 1), Subscriber.of(processorId = 0, dag))
  val task2LaunchData = TaskLaunchData(TaskId(1, 0), ProcessorDescription(task2Class,
    parallelism = 1), Subscriber.of(processorId = 1, dag))

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

    // taskmanager should return the latest clock to task(0,0)
    clockService.expectMsg(GetLatestMinClock)
    clockService.reply(LatestMinClock(0))

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

  class Task1(taskContext : TaskContext, userConf : UserConfig)
    extends Task(taskContext, userConf) {
    override def onStart(startTime: StartTime): Unit = ???
    override def onNext(msg: Message): Unit = ???
  }

  class Task2 (taskContext : TaskContext, userConf : UserConfig)
    extends Task(taskContext, userConf) {
    override def onStart(startTime: StartTime): Unit = ???
    override def onNext(msg: Message): Unit = ???
  }
}
