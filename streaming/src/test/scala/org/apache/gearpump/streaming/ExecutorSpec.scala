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


import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.MasterToAppMaster.ReplayFromTimestampWindowTrailingEdge
import org.apache.gearpump.cluster.MasterHarness

import akka.actor.{Actor, PoisonPill, Props}
import akka.testkit.TestProbe
import org.apache.gearpump.cluster.{ExecutorContext, UserConfig, TestUtil}
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.streaming.AppMasterToExecutor._
import org.apache.gearpump.streaming.ExecutorToAppMaster.RegisterExecutor
import org.apache.gearpump.streaming.task.{TaskContext, TaskLocations, TaskId}
import org.apache.gearpump.transport.HostPort
import org.scalatest._

import scala.concurrent.duration._

class ExecutorSpec extends WordSpec with Matchers with BeforeAndAfterEach with MasterHarness {
  override def config = ConfigFactory.parseString(""" akka.loggers = ["akka.testkit.TestEventListener"] """).
    withFallback(TestUtil.DEFAULT_CONFIG)

  val appId = 0
  val workerId = 1
  val executorId = 1
  val startClock = 1024
  val resource = Resource(3)
  var watcher: TestProbe = null
  var mockMaster: TestProbe = null
  var executor: TestActorRef[Executor] = null
  var executorContext: ExecutorContext = null
  var taskContext: TaskContext = null

  override def beforeEach() = {
    startActorSystem()
    mockMaster = TestProbe()(getActorSystem)
    watcher = TestProbe()(getActorSystem)
    executorContext = ExecutorContext(executorId, workerId, appId, mockMaster.ref, resource)
    taskContext = TaskContext(TaskId(0, 0), executorId, appId, mockMaster.ref, 1, DAG.empty())
    executor = TestActorRef(Props(classOf[Executor], executorContext, UserConfig.empty))(getActorSystem)
    mockMaster.expectMsg(RegisterExecutor(executor, executorId, resource, workerId))
  }

  override def afterEach() = {
    shutdownActorSystem()
  }

  "The new started executor" should {
    "launch task properly" in {
      executor.tell(LaunchTask(TaskId(0, 0), taskContext, classOf[MockTask]), watcher.ref)
      mockMaster.expectMsg(MockTaskStarted)
      val locations = TaskLocations(Map.empty[HostPort, Set[TaskId]])
      executor.tell(locations, watcher.ref)
      mockMaster.expectMsg(TaskLocationReady)
      executor.tell(RestartTasks(200), watcher.ref)
      mockMaster.expectMsg(RestartTasks(200))
    }

    "kill it self if the AppMaster is down" in {
      watcher watch executor
      mockMaster.ref ! PoisonPill
      watcher.expectTerminated(executor, 5 seconds)
    }

    "handle exception thrown from child properly" in {
      implicit val system = getActorSystem
      executor.tell(LaunchTask(TaskId(0, 0), taskContext, classOf[MockTask]), watcher.ref)
      mockMaster.expectMsg(MockTaskStarted)
      EventFilter[MsgLostException](occurrences = 1) intercept {
        executor.children.head ! MsgLost
      }
      mockMaster.expectMsg(ReplayFromTimestampWindowTrailingEdge)
      mockMaster.expectMsg(MockTaskStarted)
      EventFilter[RestartException](occurrences = 1) intercept {
        executor.children.head ! new RestartException
      }
      mockMaster.expectMsg(MockTaskStarted)
      executor.children.head ! PoisonPill
      mockMaster.expectNoMsg()
    }
  }
}

case object MockTaskStarted
case object MsgLost

class MockTask(taskContext : TaskContext, userConf : UserConfig) extends Actor {
  val appMaster = taskContext.appMaster

  override def preStart() = {
    appMaster ! MockTaskStarted
  }

  override def receive: Receive = {
    case MsgLost =>
      throw new MsgLostException
    case exception: Exception =>
      throw exception
    case msg =>
      appMaster forward msg
  }
}
