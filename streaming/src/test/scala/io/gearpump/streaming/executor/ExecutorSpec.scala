/*
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gearpump.streaming.executor

import akka.actor.{ActorRefFactory, ActorSystem, Props}
import akka.testkit.TestProbe
import io.gearpump.cluster.{ExecutorContext, TestUtil, UserConfig}
import io.gearpump.cluster.appmaster.WorkerInfo
import io.gearpump.cluster.scheduler.Resource
import io.gearpump.cluster.worker.WorkerId
import io.gearpump.serializer.SerializationFramework
import io.gearpump.streaming.{LifeTime, ProcessorDescription}
import io.gearpump.streaming.AppMasterToExecutor._
import io.gearpump.streaming.ExecutorToAppMaster.RegisterTask
import io.gearpump.streaming.appmaster.TaskRegistry.TaskLocations
import io.gearpump.streaming.executor.TaskLauncher.TaskArgument
import io.gearpump.streaming.executor.TaskLauncherSpec.MockTask
import io.gearpump.streaming.task.{Subscriber, TaskId}
import io.gearpump.transport.HostPort
import org.mockito.Matchers._
import org.mockito.Mockito.{times, _}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ExecutorSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val appId = 0
  val executorId = 0
  val workerId = WorkerId(0, 0L)
  var appMaster: TestProbe = null
  implicit var system: ActorSystem = null
  val userConf = UserConfig.empty

  override def beforeAll(): Unit = {
    system = ActorSystem("TaskLauncherSpec", TestUtil.DEFAULT_CONFIG)
    appMaster = TestProbe()
  }

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }

  it should "call launcher to launch task" in {
    val worker = TestProbe()
    val workerInfo = WorkerInfo(workerId, worker.ref)
    val executorContext = ExecutorContext(executorId, workerInfo, appId, "app",
      appMaster.ref, Resource(2))
    val taskLauncher = mock(classOf[ITaskLauncher])
    val executor = system.actorOf(Props(new Executor(executorContext, userConf, taskLauncher)))
    val processor = ProcessorDescription(id = 0, taskClass = classOf[MockTask].getName,
      parallelism = 2)
    val taskIds = List(TaskId(0, 0), TaskId(0, 1))
    val launchTasks = LaunchTasks(taskIds, dagVersion = 0, processor, List.empty[Subscriber])

    val task = TestProbe()
    when(taskLauncher.launch(any[List[TaskId]](), any[TaskArgument](), any[ActorRefFactory](),
      any[SerializationFramework](), any[String]()))
      .thenReturn(taskIds.map((_, task.ref)).toMap)

    val client = TestProbe()
    client.send(executor, launchTasks)
    client.expectMsg(TasksLaunched)

    verify(taskLauncher, times(1)).launch(any[List[TaskId]](),
      any[TaskArgument](), any[ActorRefFactory](), any[SerializationFramework](), any[String]())

    executor ! RegisterTask(TaskId(0, 0), executorId, HostPort("localhost:80"))
    executor ! RegisterTask(TaskId(0, 1), executorId, HostPort("localhost:80"))

    executor ! TaskRegistered(TaskId(0, 0), 0, 0)

    task.expectMsgType[TaskRegistered]

    executor ! TaskRegistered(TaskId(0, 1), 0, 0)

    task.expectMsgType[TaskRegistered]

    executor ! TaskLocationsReady(TaskLocations(Map.empty), dagVersion = 0)
    executor ! StartAllTasks(dagVersion = 0)

    task.expectMsgType[StartTask]
    task.expectMsgType[StartTask]

    val changeTasks = ChangeTasks(taskIds, dagVersion = 1, life = LifeTime(0, Long.MaxValue),
      List.empty[Subscriber])

    client.send(executor, changeTasks)
    client.expectMsgType[TasksChanged]

    executor ! TaskLocationsReady(TaskLocations(Map.empty), 1)
    executor ! StartAllTasks(dagVersion = 1)

    task.expectMsgType[ChangeTask]
    task.expectMsgType[ChangeTask]
  }
}