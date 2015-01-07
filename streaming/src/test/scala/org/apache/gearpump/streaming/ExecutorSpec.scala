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

import akka.actor.{Actor, PoisonPill, Props, ActorSystem}
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import org.apache.gearpump.cluster.{ExecutorContext, UserConfig, TestUtil}
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.streaming.AppMasterToExecutor._
import org.apache.gearpump.streaming.ExecutorToAppMaster.RegisterExecutor
import org.apache.gearpump.streaming.task.{TaskContext, TaskLocations, TaskId}
import org.apache.gearpump.transport.HostPort
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

class ExecutorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("ExecutorSpec"))

  val mockMaster = TestProbe()
  val watcher = TestProbe()
  val appId = 0
  val executorId = 1
  val workerId = 2
  val resource = Resource(3)
  val startClock = 1024

  val executorContext = ExecutorContext(executorId, workerId, appId, mockMaster.ref, resource)

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "The new started executor" should {
    "register itself to AppMaster when started" in {
      val system = ActorSystem("ExecutorSystem", TestUtil.DEFAULT_CONFIG)
      val executor = system.actorOf(Props(classOf[Executor], executorContext, UserConfig.empty))
      mockMaster.expectMsg(RegisterExecutor(executor, executorId, resource, workerId))
      system.shutdown()
    }

    "launch task properly" in {
      val system = ActorSystem("ExecutorSystem", TestUtil.DEFAULT_CONFIG)
      val executor = system.actorOf(Props(classOf[Executor], executorContext, UserConfig.empty))
      mockMaster.expectMsg(RegisterExecutor(executor, executorId, resource, workerId))
      val taskContext = TaskContext(TaskId(0, 0), executorId, appId, mockMaster.ref, DAG.empty())
      executor.tell(LaunchTask(TaskId(0, 0), taskContext, classOf[MockTask]), watcher.ref)
      mockMaster.expectMsg(MockTaskStarted)
      val locations = TaskLocations(Map.empty[HostPort, Set[TaskId]])
      executor.tell(locations, watcher.ref)
      mockMaster.expectMsg(TaskLocationReady)
      executor.tell(RestartTasks(200), watcher.ref)
      mockMaster.expectMsg(RestartTasks(200))
      system.shutdown()
    }

    "kill it self if the AppMaster is down" in {
      val system = ActorSystem("ExecutorSystem", TestUtil.DEFAULT_CONFIG)
      val executor = system.actorOf(Props(classOf[Executor], executorContext, UserConfig.empty))
      watcher watch executor
      mockMaster.expectMsg(RegisterExecutor(executor, executorId, resource, workerId))
      mockMaster.ref ! PoisonPill
      watcher.expectTerminated(executor, 5 seconds)
      system.shutdown()
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
    case msg =>
      appMaster forward msg
  }
}
