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

import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import akka.testkit.{TestProbe, TestActorRef}
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.TestUtil.MiniCluster
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.cluster.{TestUtil, AppMasterContext, AppMasterInfo, UserConfig}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.AppMasterToExecutor.StartClock
import org.apache.gearpump.streaming.ExecutorToAppMaster.RegisterTask
import org.apache.gearpump.streaming.task.{TaskId, StartTime, TaskActor, TaskContext}
import org.apache.gearpump.transport.Express
import org.apache.gearpump.util.Graph
import org.apache.gearpump.util.Graph._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object StreamingTestUtil {
  private var executorId = 0
  val testUserName = "testuser"

  def startAppMaster(miniCluster: MiniCluster, appId: Int,
                     app: AppDescription = AppDescription("test", classOf[AppMaster].getName, UserConfig.empty, Graph.empty)): TestActorRef[AppMaster] = {

    val masterConf = AppMasterContext(appId, testUserName, executorId, Resource.empty,  None,miniCluster.mockMaster,AppMasterInfo(miniCluster.worker))

    val props = Props(classOf[AppMaster], masterConf, app)
    executorId += 1
    miniCluster.launchActor(props).asInstanceOf[TestActorRef[AppMaster]]
  }

  def createEchoForTaskActor(taskClass: String, taskConf: UserConfig, system1: ActorSystem, system2: ActorSystem): (ActorRef, TestProbe) = {
    import system1.dispatcher
    val task1 = TaskDescription(taskClass, 1)
    val task2 = TaskDescription(classOf[EchoTask].getName, 1)
    val dag = DAG(Graph(task1 ~ new HashPartitioner() ~> task2))
    val appId = 0
    val taskReporter = TestProbe()(system2)
    val taskId1 = TaskId(0, 0)
    val taskId2 = TaskId(1, 0)
    val executorId1 = 1
    val executorId2 = 2
    val appMaster = system1.actorOf(Props(classOf[MockAppMaster]))
    val taskContext1 = TaskContext(taskId1, executorId1, appId, appMaster, dag)
    val taskContext2 = TaskContext(taskId2, executorId2, appId, appMaster, dag)
    val testActor = system1.actorOf(Props(Class.forName(taskClass), taskContext1, taskConf))
    val reporter = system2.actorOf(Props(classOf[EchoTask], taskContext2, UserConfig.empty.withValue(EchoTask.TEST_PROBE, taskReporter.ref)))
    val express1 = Express(system1)
    val express2 = Express(system2)
    val testActorHost = express1.localHost
    val reporterHost = express2.localHost
    val locations = Map((reporterHost, Set(taskId2)), (testActorHost, Set(taskId1)))
    val result = locations.flatMap { kv =>
      val (host, taskIdList) = kv
      taskIdList.map(taskId => (TaskId.toLong(taskId), host))
    }
    val future = express1.startClients(locations.keySet).map { _ =>
      express1.remoteAddressMap.send(result)
    }.map { _ =>
      express2.startClients(locations.keySet).map { _ =>
        express2.remoteAddressMap.send(result)
      }
    }
    Await.result(future, Duration(10, TimeUnit.SECONDS))
    (testActor, taskReporter)
  }
}

class EchoTask(taskContext : TaskContext, userConf : UserConfig) extends TaskActor(taskContext, userConf) {
  import EchoTask._
  var testProbe: ActorRef = null
  override def onStart(startTime: StartTime): Unit = {
    testProbe = userConf.getAnyRef(TEST_PROBE).get.asInstanceOf[ActorRef]
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