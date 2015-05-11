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
package gearpump.streaming.task

import akka.actor.{Actor, ActorRef, Props, ActorSystem}
import akka.testkit._
import com.typesafe.config.ConfigFactory
import gearpump.Message
import gearpump.streaming.{ProcessorDescription, DAG, ExecutorToAppMaster, AppMasterToExecutor}
import gearpump.cluster.{ExecutorContext, MasterHarness, UserConfig, TestUtil}
import gearpump.partitioner.HashPartitioner
import AppMasterToExecutor.{MsgLostException, RestartException, StartClock}
import ExecutorToAppMaster.{RegisterExecutor, RegisterTask}
import TaskActor.RestartTask
import TaskActorSpec.TestTask
import gearpump.transport.Express
import gearpump.util.Graph
import org.scalatest.{WordSpec, Matchers, BeforeAndAfterEach}
import gearpump.util.Graph._
import org.mockito.Mockito.{mock, when, verify, times}

class TaskActorSpec extends WordSpec with Matchers with BeforeAndAfterEach with MasterHarness {
  override def config = ConfigFactory.parseString(
    """ akka.loggers = ["akka.testkit.TestEventListener"]
      | akka.test.filter-leeway = 20000
    """.stripMargin).
    withFallback(TestUtil.DEFAULT_CONFIG)

  val appId = 0
  val task1 = ProcessorDescription(classOf[TestTask].getName, 1)
  val task2 = ProcessorDescription(classOf[TestTask].getName, 1)
  val dag = DAG(Graph(task1 ~ new HashPartitioner() ~> task2))
  val taskId1 = TaskId(0, 0)
  val taskId2 = TaskId(1, 0)
  val executorId1 = 1
  val executorId2 = 2

  var mockMaster: TestProbe = null
  var taskContext1: TaskContextData = null
  var taskContext2: TaskContextData = null

  override def beforeEach() = {
    startActorSystem()
    mockMaster = TestProbe()(getActorSystem)
    taskContext1 = TaskContextData(taskId1, executorId1, appId,
      "appName", mockMaster.ref, 1, Subscriber.of(processorId = 0, dag))
    taskContext2 = TaskContextData(taskId2, executorId2, appId,
      "appName", mockMaster.ref, 1, Subscriber.of(processorId = 1, dag))
  }

  "TaskActor" should {
    "register itself to AppMaster when started" in {

      val mockTask = mock(classOf[TaskWrapper])
      val testActor = TestActorRef[TaskActor](Props(classOf[TaskActor], taskContext1, UserConfig.empty, mockTask))(getActorSystem)
      val express1 = Express(getActorSystem)
      val testActorHost = express1.localHost
      mockMaster.expectMsg(RegisterTask(taskId1, executorId1, testActorHost))
      mockMaster.reply(StartClock(0))

      implicit val system = getActorSystem
      val ack = Ack(taskId2, 100, 99, testActor.underlyingActor.sessionId)
      EventFilter[MsgLostException](occurrences = 1) intercept {
        testActor ! ack
      }
    }

    "throw RestartException when register to AppMaster time out" in {
      implicit val system = getActorSystem
      val mockTask = mock(classOf[TaskWrapper])
      EventFilter[RestartException](occurrences = 1) intercept {
        TestActorRef[TaskActor](Props(classOf[TaskActor], taskContext1, UserConfig.empty, mockTask))(getActorSystem)
      }
    }

    "transport message to target correctly" in {
      val mockTask = mock(classOf[TaskWrapper])
      val msg = Message("test")

      val testActor = TestActorRef[TaskActor](Props(classOf[TaskActor], taskContext1, UserConfig.empty, mockTask))(getActorSystem)

      testActor.tell(StartClock(0), mockMaster.ref)

      testActor.tell(msg, testActor)

      verify(mockTask, times(1)).onNext(msg);

      implicit val system = getActorSystem

      EventFilter[RestartException](occurrences = 1) intercept {
        testActor ! RestartTask
      }
    }
  }

  override def afterEach() = {
    shutdownActorSystem()
  }
}

object TaskActorSpec {
  class TestTask
}