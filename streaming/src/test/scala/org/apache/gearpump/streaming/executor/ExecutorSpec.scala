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

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.MasterToAppMaster.ReplayFromTimestampWindowTrailingEdge
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.cluster.{ExecutorContext, MasterHarness, TestUtil, UserConfig}
import org.apache.gearpump.streaming.AppMasterToExecutor._
import org.apache.gearpump.streaming.Executor.RestartExecutor
import org.apache.gearpump.streaming.ExecutorToAppMaster.RegisterExecutor
import org.apache.gearpump.streaming.executor.ExecutorSpec.{MsgLost, MockTaskStarted, MockTask}
import org.apache.gearpump.streaming.task.TaskActor.RestartTask
import org.apache.gearpump.streaming.task.{TaskContext, TaskId, TaskLocations}
import org.apache.gearpump.streaming.{DAG, Executor, TaskLocationReady}
import org.apache.gearpump.transport.HostPort
import org.scalatest._

import scala.concurrent.duration._
import scala.language.postfixOps

class ExecutorSpec extends WordSpec with Matchers with BeforeAndAfterEach with MasterHarness {
  override def config = ConfigFactory.parseString(""" akka.loggers = ["akka.testkit.TestEventListener"] """).
    withFallback(TestUtil.DEFAULT_CONFIG)

  val appId = 0
  val workerId = 1
  val executorId = 1
  val startClock = 1024
  val resource = Resource(3)
  var watcher: TestProbe = null
  var appMaster: TestProbe = null
  var executor: TestActorRef[Executor] = null
  var executorContext: ExecutorContext = null
  var taskContext: TaskContext = null
  var task: TestProbe = null

  override def beforeEach() = {
    startActorSystem()
    implicit val system = getActorSystem
    appMaster = TestProbe()
    watcher = TestProbe()
    task = TestProbe()
    val userConfig = UserConfig.empty.withValue(ExecutorSpec.TASK_PROBE, task.ref)
    executorContext = ExecutorContext(executorId, workerId, appId, appMaster.ref, resource)
    taskContext = TaskContext(TaskId(0, 0), executorId, appId, "appName", appMaster.ref, 1, DAG.empty())
    executor = TestActorRef(Props(classOf[Executor], executorContext, userConfig))(getActorSystem)
    appMaster.expectMsg(RegisterExecutor(executor, executorId, resource, workerId))
  }

  override def afterEach() = {
    shutdownActorSystem()
  }

  "The new started executor" should {
    "launch task properly" in {
      executor.tell(LaunchTask(TaskId(0, 0), taskContext, classOf[MockTask]), watcher.ref)
      task.expectMsg(MockTaskStarted)
      val locations = TaskLocations(Map.empty[HostPort, Set[TaskId]])
      executor.tell(locations, watcher.ref)
      task.expectMsg(TaskLocationReady)

      executor.tell(RestartExecutor, watcher.ref)
      task.expectMsg(RestartTask)
    }

    "kill it self if the AppMaster is down" in {
      watcher watch executor
      appMaster.ref ! PoisonPill
      watcher.expectTerminated(executor, 5 seconds)
    }

    "handle exception thrown from child properly" in {
      implicit val system = getActorSystem
      executor.tell(LaunchTask(TaskId(0, 0), taskContext, classOf[MockTask]), watcher.ref)
      task.expectMsg(MockTaskStarted)
      EventFilter[MsgLostException](occurrences = 1) intercept {
        executor.children.head ! MsgLost
      }
      appMaster.expectMsg(ReplayFromTimestampWindowTrailingEdge)
      task.expectMsg(MockTaskStarted)
      EventFilter[RestartException](occurrences = 1) intercept {
        executor.children.head ! new RestartException
      }
      task.expectMsg(MockTaskStarted)
      executor.children.head ! PoisonPill
      appMaster.expectNoMsg()
    }
  }
}

object ExecutorSpec {
  val TASK_PROBE = "taskProbe"


  case object MockTaskStarted
  case object MsgLost


  class MockTask(taskContext : TaskContext, userConf : UserConfig) extends Actor {
    implicit val system = context.system
    val taskProbe = userConf.getValue[ActorRef](TASK_PROBE).get

    override def preStart() = {
      taskProbe ! MockTaskStarted
    }

    override def receive: Receive = {
      case MsgLost =>
        throw new MsgLostException
      case exception: Exception =>
        throw exception
      case msg =>
        taskProbe forward msg
    }
  }
}

