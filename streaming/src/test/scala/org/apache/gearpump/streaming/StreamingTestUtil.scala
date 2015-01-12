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

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.serialization.JavaSerializer
import akka.testkit.{TestProbe, TestActorRef}
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.TestUtil.MiniCluster
import org.apache.gearpump.cluster.master.AppMasterRuntimeInfo
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.cluster.{AppMasterContext, UserConfig}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.AppMasterToExecutor.StartClock
import org.apache.gearpump.streaming.ExecutorToAppMaster.RegisterTask
import org.apache.gearpump.streaming.task.{TaskId, StartTime, TaskActor, TaskContext}
import org.apache.gearpump.transport.Express
import org.apache.gearpump.util.Graph
import org.apache.gearpump.util.Graph._

import scala.concurrent.{Future, Await}
import scala.concurrent.duration.Duration

object StreamingTestUtil {
  private var executorId = 0
  val testUserName = "testuser"

  def startAppMaster(miniCluster: MiniCluster, appId: Int,
                     app: AppDescription = AppDescription("test", classOf[AppMaster].getName, UserConfig.empty, Graph.empty)): TestActorRef[AppMaster] = {

    val masterConf = AppMasterContext(appId, testUserName, executorId, Resource.empty,  None,miniCluster.mockMaster,AppMasterRuntimeInfo(miniCluster.worker))

    val props = Props(classOf[AppMaster], masterConf, app)
    executorId += 1
    miniCluster.launchActor(props).asInstanceOf[TestActorRef[AppMaster]]
  }

  /**
   * This function is a work-around for testing.
   * The goal of this method to to verify the output message from target actor to test.
   *
   * It works by creating a EchoActor as downstream of target actor, and the Echo Actor will relay all
   * messages to a TestProbe, which will be returned to user when this method returns.
   *
   * @param taskClass the task to test
   * @param taskConf  the configuration passed to the task to test
   * @param system1   system will launch the task to test
   * @param system2   system will launch the Echo Actor
   *
   * @return (taskActor: TestActorRef, echoTask: TestProbe)
   *         taskActor: the TestActorRef of the task to test
   *         echoTask: the TestProbe will receive the message sent from the task
   */
  def createEchoForTaskActor(taskClass: String, taskConf: UserConfig, system1: ActorSystem, system2: ActorSystem): (TestActorRef[TaskActor], TestProbe) = {
    import system1.dispatcher
    val taskToTest = TaskDescription(taskClass, 1)
    val echoTask = TaskDescription(classOf[EchoTask].getName, 1)
    val dag = DAG(Graph(taskToTest ~ new HashPartitioner() ~> echoTask))
    val taskReporter = TestProbe()(system2)
    val taskId1 = TaskId(0, 0)
    val taskId2 = TaskId(1, 0)
    val appMaster = system1.actorOf(Props(classOf[MockAppMaster]))

    implicit val systemForSerializer = system1.asInstanceOf[ExtendedActorSystem]

    val testActor = TestActorRef[TaskActor](Props(Class.forName(taskClass), TaskContext(taskId1, 1, 0, appMaster, 1, dag), taskConf))(system1)
    val reporter = system2.actorOf(Props(classOf[EchoTask], TaskContext(taskId2, 2, 0, appMaster, 1, dag), UserConfig.empty.withValue(EchoTask.TEST_PROBE, taskReporter.ref)))

    val express1 = Express(system1)
    val express2 = Express(system2)
    val testActorHost = express1.localHost
    val reporterHost = express2.localHost
    val locations = Map((reporterHost, Set(taskId2)), (testActorHost, Set(taskId1)))
    val result = locations.flatMap { kv =>
      val (host, taskIdList) = kv
      taskIdList.map(taskId => (TaskId.toLong(taskId), host))
    }
    val future1 = express1.startClients(locations.keySet).map { _ =>
      express1.remoteAddressMap.send(result)
    }
    val future2 = express2.startClients(locations.keySet).map { _ =>
      express2.remoteAddressMap.send(result)
    }
    val future = Future.sequence(List(future1, future2))
    Await.result(future, Duration(10, TimeUnit.SECONDS))
    (testActor, taskReporter)
  }
}

class EchoTask(taskContext : TaskContext, userConf : UserConfig) extends TaskActor(taskContext, userConf) {
  import EchoTask._
  var testProbe: ActorRef = null
  implicit val actorSystem: ExtendedActorSystem = context.system.asInstanceOf[ExtendedActorSystem]

  override def onStart(startTime: StartTime): Unit = {

    testProbe = userConf.getValue[ActorRef](TEST_PROBE).get
  }

  override def onNext(msg: Message): Unit = {
    testProbe forward Message(msg.msg)
  }
}

object EchoTask {
  val TEST_PROBE = "test_probe"
}

class MockAppMaster extends Actor {
  override def receive: Receive = {
    case msg: RegisterTask =>
      sender ! StartClock(0)
  }
}
