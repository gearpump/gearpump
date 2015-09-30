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
package io.gearpump.streaming.task

import akka.actor.{ExtendedActorSystem, Props}
import akka.testkit._
import com.typesafe.config.ConfigFactory
import io.gearpump.Message
import io.gearpump.cluster.{MasterHarness, TestUtil, UserConfig}
import io.gearpump.partitioner.{HashPartitioner, Partitioner}
import io.gearpump.serializer.{SerializerPool, FastKryoSerializer}
import io.gearpump.streaming.AppMasterToExecutor.{ChangeTask, MsgLostException, RegisterTaskFailedException, Start, TaskChanged, TaskRejected}
import io.gearpump.streaming.ExecutorToAppMaster.RegisterTask
import io.gearpump.streaming.task.TaskActorSpec.TestTask
import io.gearpump.streaming.{DAG, LifeTime, ProcessorDescription}
import io.gearpump.transport.Express
import io.gearpump.util.Graph._
import io.gearpump.util.{Graph, Util}
import org.mockito.Mockito.{mock, times, verify, when}
import org.scalatest.{BeforeAndAfterEach, Matchers, WordSpec}


class TaskActorSpec extends WordSpec with Matchers with BeforeAndAfterEach with MasterHarness {
  override def config = ConfigFactory.parseString(
    """ akka.loggers = ["akka.testkit.TestEventListener"]
      | akka.test.filter-leeway = 20000
    """.stripMargin).
    withFallback(TestUtil.DEFAULT_CONFIG)

  val appId = 0
  val task1 = ProcessorDescription(id = 0, taskClass = classOf[TestTask].getName, parallelism = 1)
  val task2 = ProcessorDescription(id = 1, taskClass = classOf[TestTask].getName, parallelism = 1)
  val dag: DAG = DAG(Graph(task1 ~ Partitioner[HashPartitioner] ~> task2))
  val taskId1 = TaskId(0, 0)
  val taskId2 = TaskId(1, 0)
  val executorId1 = 1
  val executorId2 = 2

  var mockMaster: TestProbe = null
  var taskContext1: TaskContextData = null

  var mockSerializerPool: SerializerPool = null

  override def beforeEach() = {
    startActorSystem()
    mockMaster = TestProbe()(getActorSystem)

    mockSerializerPool = mock(classOf[SerializerPool])
    val serializer = new FastKryoSerializer(getActorSystem.asInstanceOf[ExtendedActorSystem])
    when(mockSerializerPool.get()).thenReturn(serializer)

    taskContext1 = TaskContextData(executorId1, appId,
      "appName", mockMaster.ref, 1,
      LifeTime.Immortal,
      Subscriber.of(processorId = 0, dag))
  }

  "TaskActor" should {
    "register itself to AppMaster when started" in {

      val mockTask = mock(classOf[TaskWrapper])
      val testActor = TestActorRef[TaskActor](Props(new TaskActor(taskId1, taskContext1, UserConfig.empty, mockTask, mockSerializerPool)))(getActorSystem)
      val express1 = Express(getActorSystem)
      val testActorHost = express1.localHost
      mockMaster.expectMsg(RegisterTask(taskId1, executorId1, testActorHost))
      mockMaster.reply(Start(0, Util.randInt))

      implicit val system = getActorSystem
      val ack = Ack(taskId2, 100, 99, testActor.underlyingActor.sessionId)
      EventFilter[MsgLostException](occurrences = 1) intercept {
        testActor ! ack
      }
    }

    "respond to ChangeTask" in {

      val mockTask = mock(classOf[TaskWrapper])
      val testActor = TestActorRef[TaskActor](Props(new TaskActor(taskId1, taskContext1, UserConfig.empty, mockTask, mockSerializerPool)))(getActorSystem)
      val express1 = Express(getActorSystem)
      val testActorHost = express1.localHost
      mockMaster.expectMsg(RegisterTask(taskId1, executorId1, testActorHost))
      mockMaster.reply(Start(0, Util.randInt))

      mockMaster.send(testActor, ChangeTask(taskId1, 1, LifeTime.Immortal, List.empty[Subscriber]))
      mockMaster.expectMsgType[TaskChanged]
    }

    "shutdown itself when RegisterTask is rejected" in {
      val mockTask = mock(classOf[TaskWrapper])
      val testActor = TestActorRef[TaskActor](Props(new TaskActor(taskId1, taskContext1, UserConfig.empty, mockTask, mockSerializerPool)))(getActorSystem)
      val express1 = Express(getActorSystem)
      val testActorHost = express1.localHost
      mockMaster.expectMsg(RegisterTask(taskId1, executorId1, testActorHost))

      implicit val system = getActorSystem
      val deathWatch = TestProbe()
      deathWatch.watch(testActor)
      mockMaster.reply(TaskRejected)
      import scala.concurrent.duration._
      deathWatch.expectTerminated(testActor, 15 seconds)
    }

    "throw RegisterTaskFailedException when register to AppMaster time out" in {
      implicit val system = getActorSystem
      val mockTask = mock(classOf[TaskWrapper])
      EventFilter[RegisterTaskFailedException](occurrences = 1) intercept {
        TestActorRef[TaskActor](Props(new TaskActor(taskId1, taskContext1, UserConfig.empty, mockTask, mockSerializerPool)))(getActorSystem)
      }
    }

    "handle received message correctly" in {
      val mockTask = mock(classOf[TaskWrapper])
      val msg = Message("test")

      val testActor = TestActorRef[TaskActor](Props(new TaskActor(taskId1, taskContext1, UserConfig.empty, mockTask, mockSerializerPool)))(getActorSystem)

      testActor.tell(Start(0, Util.randInt), mockMaster.ref)

      testActor.tell(msg, testActor)

      verify(mockTask, times(1)).onNext(msg)
    }
  }

  override def afterEach() = {
    shutdownActorSystem()
  }
}

object TaskActorSpec {
  class TestTask
}