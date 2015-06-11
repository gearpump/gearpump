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
import org.apache.gearpump.cluster.MasterToAppMaster.ReplayFromTimestampWindowTrailingEdge
import org.apache.gearpump.cluster.scheduler.{Resource, ResourceRequest}
import org.apache.gearpump.cluster.{TestUtil, UserConfig}
import org.apache.gearpump.partitioner.{PartitionerDescription, HashPartitioner, Partitioner}
import org.apache.gearpump.streaming.AppMasterToExecutor.{StartClock, LaunchTasks}
import org.apache.gearpump.streaming.appmaster.ClockService.{ChangeToNewDAGSuccess, ChangeToNewDAG}
import org.apache.gearpump.streaming.appmaster.DagManager.{NewDAGDeployed, GetTaskLaunchData, WatchChange, LatestDAG, GetLatestDAG, TaskLaunchData}
import org.apache.gearpump.partitioner.{HashPartitioner, Partitioner}
import org.apache.gearpump.streaming.ExecutorToAppMaster.RegisterTask
import org.apache.gearpump.streaming.appmaster.AppMaster.AllocateResourceTimeOut
import org.apache.gearpump.streaming.appmaster.ExecutorManager._
import org.apache.gearpump.streaming.appmaster.TaskManagerSpec.{Env, Task1, Task2}
import org.apache.gearpump.streaming.executor.Executor.RestartTasks
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.streaming.{LifeTime, DAG, ProcessorDescription}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Graph
import org.apache.gearpump.util.Graph._
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.mockito.Matchers._

class TaskManagerSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  implicit var system: ActorSystem = null

  val task1Class = classOf[Task1].getName
  val task2Class = classOf[Task2].getName

  val task1 = ProcessorDescription(id = 0, task1Class, 1)
  val task2 = ProcessorDescription(id = 1, task2Class, 1)

  val dag: DAG = DAG(Graph(task1 ~ Partitioner[HashPartitioner] ~> task2))
  val dagVersion = 0

  val task1LaunchData = TaskLaunchData(task1, Subscriber.of(processorId = 0, dag))
  val task2LaunchData = TaskLaunchData(task2, Subscriber.of(processorId = 1, dag))

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

  it should "recover by requesting new executors when executor stopped unexpectedly" in {
    val env = bootUp
    import env._

    val resourceRequest = Array(ResourceRequest(resource, workerId))
    when(scheduler.executorFailed(executorId)).thenReturn(resourceRequest)


    taskManager ! ExecutorStopped(executorId)

    // when one executor stop, it will also trigger the recovery by restart
    // existing executors
    executorManager.expectMsg(BroadCast(RestartTasks(dagVersion)))

    // ask for new executors
    val returned = executorManager.expectMsg(StartExecutors(resourceRequest))
    executorManager.reply(StartExecutorsTimeOut)

    // TaskManager cannot handle the TimeOut error itself, escalate to appmaster.
    appMaster.expectMsg(AllocateResourceTimeOut)
  }

  it should "recover by restarting existing executors when message loss happen" in {
    val env = bootUp
    import env._

    taskManager ! ReplayFromTimestampWindowTrailingEdge(appId)

    // Restart the executors so that we can replay from minClock
    executorManager.expectMsg(BroadCast(RestartTasks(dagVersion)))
  }

  import org.apache.gearpump.streaming.appmaster.TaskManager.TaskChangeRegistry
  "TaskChangeRegistry" should "track all modified task registration" in {
    val tasks = List(TaskId(0, 0), TaskId(0, 1))
    val registry = new TaskChangeRegistry(tasks)
    registry.taskChanged(TaskId(0, 0))
    registry.taskChanged(TaskId(0, 1))
    assert(registry.allTaskChanged)
  }

  "DAGDiff" should "track all the DAG migration impact" in {
    val defaultEdge = PartitionerDescription(null)
    val a = ProcessorDescription(id = 1, taskClass = null, parallelism = 1)
    val b = ProcessorDescription(id = 2, taskClass = null, parallelism = 1)
    val c = ProcessorDescription(id = 3, taskClass = null, parallelism = 1)
    val left = Graph(a ~ defaultEdge ~> b, a ~ defaultEdge ~> c)

    val d = ProcessorDescription(id = 4, taskClass = null, parallelism = 1)
    val right = left.copy
    right.addVertex(d)
    right.addEdge(c, defaultEdge, d)
    val e = a.copy(life = LifeTime(0, 0))
    right.replaceVertex(a, e)

    val diff = TaskManager.migrate(DAG(left), DAG(right, version = 1))
    diff.addedProcessors shouldBe List(d.id)

    diff.modifiedProcessors shouldBe List(a.id)

    diff.impactedUpstream shouldBe List(c.id)
  }

  private def bootUp: Env = {
    val executorManager = TestProbe()
    val clockService = TestProbe()
    val appMaster = TestProbe()
    val executor = TestProbe()

    val scheduler = mock(classOf[TaskScheduler])

    val dagManager = TestProbe()

    val taskManager = system.actorOf(
      Props(new TaskManager(appId, dagManager.ref, scheduler, executorManager.ref, clockService.ref, appMaster.ref, "appName")))

    dagManager.expectMsgType[WatchChange]
    executorManager.expectMsgType[SetTaskManager]

    // step1: first transition from Unitialized to ApplicationReady
    executorManager.expectMsgType[ExecutorResourceUsageSummary]
    dagManager.expectMsgType[NewDAGDeployed]


    // step2: DAG changed. Start transit from ApplicationReady -> DynamicDAG
    dagManager.expectMsg(GetLatestDAG)
    dagManager.reply(LatestDAG(dag))

    // step3: Get Additional Resource Request
    when(scheduler.getResourceRequests())
      .thenReturn(Array(ResourceRequest(resource)))

    // step4: Start remote Executors.
    executorManager.expectMsgType[StartExecutors]

    when(scheduler.schedule(workerId, executorId, resource))
      .thenReturn(List(TaskId(0, 0), TaskId(1, 0)))

    // step5: Executor is started.
    executorManager.reply(ExecutorStarted(executorId, resource, workerId))

    // step6: Prepare to start Task. First GetTaskLaunchData.
    val taskLaunchData: PartialFunction[Any, TaskLaunchData] = {
      case GetTaskLaunchData(_, 0, executorStarted) =>
        task1LaunchData.copy(context = executorStarted)
      case GetTaskLaunchData(_, 1, executorStarted) =>
        task2LaunchData.copy(context = executorStarted)
    }

    val launchData1 = dagManager.expectMsgPF()(taskLaunchData)
    dagManager.reply(launchData1)

    val launchData2 = dagManager.expectMsgPF()(taskLaunchData)
    dagManager.reply(launchData2)

    // step7: Launch Task
    val launchTaskMatch: PartialFunction[Any, RegisterTask] = {
      case UniCast(executorId, launch: LaunchTasks) =>
        Console.println("Launch Task " + launch.processorDescription.id)
        RegisterTask(launch.taskId.head, executorId, HostPort("127.0.0.1:3000"))
    }


    // step8: Task is started. registerTask.
    val registerTask1 = executorManager.expectMsgPF()(launchTaskMatch)
    executorManager.reply(registerTask1)

    // taskmanager should return the latest start clock to task(0,0)
    clockService.expectMsg(GetLatestStartClock)

    val registerTask2 = executorManager.expectMsgPF()(launchTaskMatch)
    executorManager.reply(registerTask2)

    // Tell executor Manager the updated usage status of executors.
    executorManager.expectMsgType[ExecutorResourceUsageSummary]

    // step9: Tell ClockService to update DAG.
    clockService.expectMsgType[ChangeToNewDAG]
    clockService.reply(ChangeToNewDAGSuccess)

    // step10: After we get reply from ClockService, start broadcasting TaskLocations.
    import scala.concurrent.duration._
    assert(executorManager.expectMsgPF(5 seconds) {
      case BroadCast(taskLocation) => taskLocation.isInstanceOf[TaskLocations]
    })

    // step11: transition from DynamicDAG to ApplicationReady
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
