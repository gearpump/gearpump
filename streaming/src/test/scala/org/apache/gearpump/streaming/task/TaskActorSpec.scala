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

import akka.actor.{Actor, ActorRef, Props, ActorSystem}
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.{ExecutorContext, MasterHarness, UserConfig, TestUtil}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.AppMasterToExecutor.{MsgLostException, RestartException, RestartTasks, StartClock}
import org.apache.gearpump.streaming.ExecutorToAppMaster.{RegisterExecutor, RegisterTask}
import org.apache.gearpump.streaming.{Executor, StreamingTestUtil, DAG, TaskDescription}
import org.apache.gearpump.transport.Express
import org.apache.gearpump.util.Graph
import org.scalatest._
import org.apache.gearpump.util.Graph._

class TaskActorSpec extends WordSpec with Matchers with BeforeAndAfterEach with MasterHarness {
  override def config = ConfigFactory.parseString(
    """ akka.loggers = ["akka.testkit.TestEventListener"]
      | akka.test.filter-leeway = 20000
    """.stripMargin).
    withFallback(TestUtil.DEFAULT_CONFIG)

  val appId = 0
  val task1 = TaskDescription(classOf[TestActor].getName, 1)
  val task2 = TaskDescription(classOf[TestActor].getName, 1)
  val dag = DAG(Graph(task1 ~ new HashPartitioner() ~> task2))
  val taskId1 = TaskId(0, 0)
  val taskId2 = TaskId(1, 0)
  val executorId1 = 1
  val executorId2 = 2

  var mockMaster: TestProbe = null
  var taskContext1: TaskContext = null
  var taskContext2: TaskContext = null

  override def beforeEach() = {
    startActorSystem()
    mockMaster = TestProbe()(getActorSystem)
    taskContext1 = TaskContext(taskId1, executorId1, appId, mockMaster.ref, 1, dag)
    taskContext2 = TaskContext(taskId2, executorId2, appId, mockMaster.ref, 1, dag)
  }

  "TaskActor" should {
    "register itself to AppMaster when started" in {
      val testActor = TestActorRef[TestActor](Props(classOf[TestActor], taskContext1, UserConfig.empty))(getActorSystem)
      val express1 = Express(getActorSystem)
      val testActorHost = express1.localHost
      mockMaster.expectMsg(RegisterTask(taskId1, executorId1, testActorHost))
      mockMaster.reply(StartClock(0))

      implicit val system = getActorSystem
      val ack = Ack(taskId1, Seq(0, 100), 99, testActor.underlyingActor.sessionId)
      EventFilter[MsgLostException](occurrences = 1) intercept {
        testActor ! ack
      }
    }

    "throw RestartException when register to AppMaster time out" in {
      implicit val system = getActorSystem
      EventFilter[RestartException](occurrences = 1) intercept {
        TestActorRef[TestActor](Props(classOf[TestActor], taskContext1, UserConfig.empty))(getActorSystem)
      }
    }

    "transport message to target correctly" in {
      val system1 = ActorSystem("TestActor", config)
      val system2 = ActorSystem("Reporter", config)
      val (testActor, echo) = StreamingTestUtil.createEchoForTaskActor(classOf[TestActor].getName, UserConfig.empty, system1, system2)
      testActor.tell(Message("test"), testActor)
      echo.expectMsg(Message("test"))
      implicit val system = system1
      EventFilter[RestartException](occurrences = 1) intercept {
        testActor ! RestartTasks(0)
      }
      system1.shutdown()
      system2.shutdown()
    }
  }

  override def afterEach() = {
    shutdownActorSystem()
  }
}

class TestActor(taskContext : TaskContext, userConf : UserConfig) extends TaskActor(taskContext, userConf) {
  override def onStart(startTime: StartTime): Unit = {
  }

  override def onNext(msg: Message): Unit = {
    output(msg)
  }
}
