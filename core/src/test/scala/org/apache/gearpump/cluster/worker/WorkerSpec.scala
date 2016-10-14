/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.cluster.worker

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.testkit.TestProbe
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.gearpump.cluster.AppMasterToWorker.{ChangeExecutorResource, LaunchExecutor, ShutdownExecutor}
import org.apache.gearpump.cluster.MasterToWorker.{UpdateResourceFailed, WorkerRegistered}
import org.apache.gearpump.cluster.WorkerToAppMaster.{ExecutorLaunchRejected, ShutdownExecutorFailed, ShutdownExecutorSucceed}
import org.apache.gearpump.cluster.WorkerToMaster.{RegisterNewWorker, RegisterWorker, ResourceUpdate}
import org.apache.gearpump.cluster.master.Master.MasterInfo
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.cluster.{ExecutorJVMConfig, MasterHarness, TestUtil}
import org.apache.gearpump.util.{ActorSystemBooter, ActorUtil, Constants}
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

class WorkerSpec extends WordSpec with Matchers with BeforeAndAfterEach with MasterHarness {
  override def config: Config = TestUtil.DEFAULT_CONFIG

  val appId = 1
  val workerId: WorkerId = WorkerId(1, 0L)
  val executorId = 1
  var masterProxy: TestProbe = null
  var mockMaster: TestProbe = null
  var client: TestProbe = null
  val workerSlots = 50

  override def beforeEach(): Unit = {
    startActorSystem()
    mockMaster = TestProbe()(getActorSystem)
    masterProxy = TestProbe()(getActorSystem)
    client = TestProbe()(getActorSystem)
  }

  override def afterEach(): Unit = {
    shutdownActorSystem()
  }

  "The new started worker" should {
    "kill itself if no response from Master after registering" in {
      val worker = getActorSystem.actorOf(Props(classOf[Worker], mockMaster.ref))
      mockMaster watch worker
      mockMaster.expectMsg(RegisterNewWorker)
      mockMaster.expectTerminated(worker, 60.seconds)
    }
  }

  "Worker" should {
    "init its resource from the gearpump config" in {
      val config = ConfigFactory.parseString(s"${Constants.GEARPUMP_WORKER_SLOTS} = $workerSlots").
        withFallback(TestUtil.DEFAULT_CONFIG)
      val workerSystem = ActorSystem("WorkerSystem", config)
      val worker = workerSystem.actorOf(Props(classOf[Worker], mockMaster.ref))
      mockMaster watch worker
      mockMaster.expectMsg(RegisterNewWorker)

      worker.tell(WorkerRegistered(workerId, MasterInfo(mockMaster.ref)), mockMaster.ref)
      mockMaster.expectMsg(ResourceUpdate(worker, workerId, Resource(workerSlots)))

      worker.tell(
        UpdateResourceFailed("Test resource update failed", new Exception()), mockMaster.ref)
      mockMaster.expectTerminated(worker, 5.seconds)
      workerSystem.terminate()
      Await.result(workerSystem.whenTerminated, Duration.Inf)
    }
  }

  "Worker" should {
    "update its remaining resource when launching and shutting down executors" in {
      val worker = getActorSystem.actorOf(Props(classOf[Worker], masterProxy.ref))
      masterProxy.expectMsg(RegisterNewWorker)

      worker.tell(WorkerRegistered(workerId, MasterInfo(mockMaster.ref)), mockMaster.ref)
      mockMaster.expectMsg(ResourceUpdate(worker, workerId, Resource(100)))

      val executorName = ActorUtil.actorNameForExecutor(appId, executorId)
      // This is an actor path which the ActorSystemBooter will report back to,
      // not needed in this test
      val reportBack = "dummy"
      val executionContext = ExecutorJVMConfig(Array.empty[String],
        getActorSystem.settings.config.getString(Constants.GEARPUMP_APPMASTER_ARGS).split(" "),
        classOf[ActorSystemBooter].getName, Array(executorName, reportBack), None,
        username = "user")

      // Test LaunchExecutor
      worker.tell(LaunchExecutor(appId, executorId, Resource(101), executionContext),
        mockMaster.ref)
      mockMaster.expectMsg(ExecutorLaunchRejected("There is no free resource on this machine"))

      worker.tell(LaunchExecutor(appId, executorId, Resource(5), executionContext), mockMaster.ref)
      mockMaster.expectMsg(ResourceUpdate(worker, workerId, Resource(95)))

      worker.tell(ChangeExecutorResource(appId, executorId, Resource(2)), client.ref)
      mockMaster.expectMsg(ResourceUpdate(worker, workerId, Resource(98)))

      // Test terminationWatch
      worker.tell(ShutdownExecutor(appId, executorId, "Test shut down executor"), client.ref)
      mockMaster.expectMsg(ResourceUpdate(worker, workerId, Resource(100)))
      client.expectMsg(ShutdownExecutorSucceed(1, 1))

      worker.tell(ShutdownExecutor(appId, executorId + 1, "Test shut down executor"), client.ref)
      client.expectMsg(ShutdownExecutorFailed(
        s"Can not find executor ${executorId + 1} for app $appId"))

      mockMaster.ref ! PoisonPill
      masterProxy.expectMsg(RegisterWorker(workerId))
    }
  }
}
