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
package org.apache.gearpump.experiments.cluster.executor

import akka.actor.{PoisonPill, Actor, Props, ActorSystem}
import akka.testkit.TestProbe
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.cluster.{ExecutorContext, UserConfig, TestUtil}
import org.apache.gearpump.experiments.cluster.AppMasterToExecutor.{MsgToTask, LaunchTask}
import org.apache.gearpump.experiments.cluster.ExecutorToAppMaster.{ResponsesFromTasks, RegisterExecutor}
import org.apache.gearpump.experiments.cluster.task.{TaskContext, TaskContextInterface}
import org.scalatest.{Matchers, WordSpec}

class DefaultExecutorSpec extends WordSpec with Matchers {

  "DefaultExecutor" should {
    "launch task and deliver message properly" in {
      val executorId = 1
      val workerId = 2
      val appId = 0
      val resource = Resource(2)
      val system = ActorSystem("DefaultExecutor", TestUtil.DEFAULT_CONFIG)
      val mockMaster = TestProbe()(system)
      val executorContext = ExecutorContext(executorId, workerId, appId, mockMaster.ref, resource)
      val executor = system.actorOf(Props(classOf[DefaultExecutor], executorContext, UserConfig.empty))
      mockMaster.expectMsg(RegisterExecutor(executor, executorId, resource, workerId))
      executor ! LaunchTask(TaskContext(executorId, appId, mockMaster.ref), classOf[MockActor], UserConfig.empty)
      executor ! LaunchTask(TaskContext(executorId, appId, mockMaster.ref), classOf[MockActor], UserConfig.empty)

      executor.tell(MsgToTask(GetName), mockMaster.ref)
      mockMaster.expectMsg(ResponsesFromTasks(executorId, List("MOCKACTOR", "MOCKACTOR")))

      val watcher = TestProbe()(system)
      watcher watch executor
      mockMaster.ref ! PoisonPill
      watcher.expectTerminated(executor)
      system.shutdown()
    }
  }
}

case object GetName

class MockActor(taskContext : TaskContextInterface, userConf : UserConfig) extends Actor {
  override def receive: Receive = {
    case GetName =>
      sender ! "MOCKACTOR"
  }
}