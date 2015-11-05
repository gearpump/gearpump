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
package io.gearpump.cluster.worker

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import io.gearpump.cluster.WorkerToMaster.RegisterNewWorker
import io.gearpump.cluster.{TestUtil, MasterHarness}
import io.gearpump.util.{ActorSystemBooter, ActorUtil, Constants}
import io.gearpump.cluster.AppMasterToWorker.{ChangeExecutorResource, LaunchExecutor, ShutdownExecutor}
import io.gearpump.cluster.MasterToWorker.{UpdateResourceFailed, WorkerRegistered}
import io.gearpump.cluster.WorkerToAppMaster.{ExecutorLaunchRejected, ShutdownExecutorFailed, ShutdownExecutorSucceed}
import io.gearpump.cluster.WorkerToMaster.{RegisterNewWorker, RegisterWorker, ResourceUpdate}
import io.gearpump.cluster.master.Master.MasterInfo
import io.gearpump.cluster.scheduler.Resource
import io.gearpump.cluster.{ExecutorJVMConfig, MasterHarness, TestUtil}
import io.gearpump.util.{ActorSystemBooter, ActorUtil, Constants}
import org.scalatest._

import scala.concurrent.duration._
import scala.language.postfixOps

class WorkerSpec extends WordSpec with Matchers with BeforeAndAfterEach with MasterHarness {
  override def config = TestUtil.DEFAULT_CONFIG

  val appId = 1
  val workerId = 1
  val executorId = 1
  var masterProxy: TestProbe = null
  var mockMaster: TestProbe = null
  var client: TestProbe = null
  val workerSlots = 50

  override def beforeEach() = {
    startActorSystem()
    mockMaster = TestProbe()(getActorSystem)
    masterProxy = TestProbe()(getActorSystem)
    client = TestProbe()(getActorSystem)
  }

  override def afterEach() = {
    shutdownActorSystem()
  }

  "The new started worker" should {
    "kill itself if no response from Master after registering" in {
      val worker = getActorSystem.actorOf(Props(classOf[Worker], mockMaster.ref))
      mockMaster watch worker
      mockMaster.expectMsg(RegisterNewWorker)
      mockMaster.expectTerminated(worker, 60 seconds)
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

      worker.tell(UpdateResourceFailed("Test resource update failed", new Exception()), mockMaster.ref)
      mockMaster.expectTerminated(worker, 5 seconds)
      workerSystem.shutdown()
    }
  }

  "Worker" should {
    "update its remaining resource when launching and shutting down executors" in {
      val worker = getActorSystem.actorOf(Props(classOf[Worker], masterProxy.ref))
      masterProxy.expectMsg(RegisterNewWorker)

      worker.tell(WorkerRegistered(workerId, MasterInfo(mockMaster.ref)), mockMaster.ref)
      mockMaster.expectMsg(ResourceUpdate(worker, workerId, Resource(100)))

      val executorName = ActorUtil.actorNameForExecutor(appId, executorId)
      val reportBack = "dummy"   //This is an actor path which the ActorSystemBooter will report back to, not needed in this test.
      val executionContext = ExecutorJVMConfig(Array.empty[String], getActorSystem.settings.config.getString(Constants.GEARPUMP_APPMASTER_ARGS).split(" "), classOf[ActorSystemBooter].getName, Array(executorName, reportBack), None, username = "user")

      //Test LaunchExecutor
      worker.tell(LaunchExecutor(appId, executorId, Resource(101), executionContext), mockMaster.ref)
      mockMaster.expectMsg(ExecutorLaunchRejected("There is no free resource on this machine"))

      worker.tell(LaunchExecutor(appId, executorId, Resource(5), executionContext), mockMaster.ref)
      mockMaster.expectMsg(ResourceUpdate(worker, workerId, Resource(95)))

      worker.tell(ChangeExecutorResource(appId, executorId, Resource(2)), client.ref)
      mockMaster.expectMsg(ResourceUpdate(worker, workerId, Resource(98)))

      //Test terminationWatch
      worker.tell(ShutdownExecutor(appId, executorId, "Test shut down executor"), client.ref)
      mockMaster.expectMsg(ResourceUpdate(worker, workerId, Resource(100)))
      client.expectMsg(ShutdownExecutorSucceed(1, 1))

      worker.tell(ShutdownExecutor(appId, executorId + 1, "Test shut down executor"), client.ref)
      client.expectMsg(ShutdownExecutorFailed(s"Can not find executor ${executorId + 1} for app $appId"))

      mockMaster.ref ! PoisonPill
      masterProxy.expectMsg(RegisterWorker(workerId))
    }
  }
}
