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
package org.apache.gearpump.streaming.task

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, Props, ActorSystem}
import akka.testkit.{EventFilter, TestProbe, ImplicitSender, TestKit}
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.{UserConfig, TestUtil}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.AppMasterToExecutor.{RestartException, RestartTasks, StartClock}
import org.apache.gearpump.streaming.ExecutorToAppMaster.RegisterTask
import org.apache.gearpump.streaming.{StreamingTestUtil, DAG, TaskDescription}
import org.apache.gearpump.transport.Express
import org.apache.gearpump.util.Graph
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.apache.gearpump.util.Graph._

import scala.concurrent.duration.FiniteDuration

class TaskActorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("TaskActorSpec"))

  val task1 = TaskDescription(classOf[TestActor].getName, 1)
  val task2 = TaskDescription(classOf[TestActor].getName, 1)
  val dag = DAG(Graph(task1 ~ new HashPartitioner() ~> task2))

  val appId = 0
  val mockMaster = TestProbe()
  val taskReporter = TestProbe()
  val appMaster = system.actorOf(Props(classOf[MockAppMaster], mockMaster.ref))
  val taskId1 = TaskId(0, 0)
  val taskId2 = TaskId(1, 0)
  val executorId1 = 1
  val executorId2 = 2

  val taskContext1 = TaskContext(taskId1, executorId1, appId, appMaster, dag)
  val taskContext2 = TaskContext(taskId2, executorId2, appId, appMaster, dag)

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "TaskActor" should {
    "register itself to AppMaster when started" in {
      val system1 = ActorSystem("TestActor", TestUtil.DEFAULT_CONFIG)
      system1.actorOf(Props(classOf[TestActor], taskContext1, UserConfig.empty))
      val express1 = Express(system1)
      val testActorHost = express1.localHost
      mockMaster.expectMsg(RegisterTask(taskId1, executorId1, testActorHost))
      system1.shutdown()
    }

    "transport message to target correctly" in {
      val system1 = ActorSystem("TestActor", TestUtil.DEFAULT_CONFIG)
      val system2 = ActorSystem("Reporter", TestUtil.DEFAULT_CONFIG)
      val (testActor, echo) = StreamingTestUtil.createEchoForTaskActor(classOf[TestActor].getName, UserConfig.empty, system1, system2)
      testActor.tell(Message("test"), testActor)
      echo.expectMsg(Message("test"))
      system1.shutdown()
      system2.shutdown()
    }
  }
}

class TestActor(taskContext : TaskContext, userConf : UserConfig) extends TaskActor(taskContext, userConf) {
  override def onStart(startTime: StartTime): Unit = {
  }

  override def onNext(msg: Message): Unit = {
    output(msg)
  }
}

class MockAppMaster(testProbe: ActorRef) extends Actor {
  override def receive: Receive = {
    case msg: RegisterTask =>
      testProbe forward msg
      sender ! StartClock(0)
  }
}
