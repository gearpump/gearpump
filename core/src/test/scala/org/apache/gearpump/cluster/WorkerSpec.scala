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
package org.apache.gearpump.cluster

import akka.actor.{PoisonPill, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.AppMasterToWorker.{ShutdownExecutor, LaunchExecutor}
import org.apache.gearpump.cluster.MasterToWorker.{UpdateResourceFailed, WorkerRegistered}
import org.apache.gearpump.cluster.WorkerToAppMaster.ExecutorLaunchRejected
import org.apache.gearpump.cluster.WorkerToMaster.{RegisterWorker, RegisterNewWorker, ResourceUpdate}
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.util.{ActorSystemBooter, ActorUtil, Constants, Util}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

class WorkerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("WorkerSpec"))

  val appId = 1
  val workerId = 1
  val executorId = 1
  val masterProxy = TestProbe()
  val mockMaster = TestProbe()
  val workerSlots = 50

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "The new started worker" should {
    "kill itself if no response from Master after registering" in {
      val workerSystem = ActorSystem("WorkerSystem", TestUtil.TEST_CONFIG)
      val worker = workerSystem.actorOf(Props(classOf[Worker], mockMaster.ref))
      mockMaster watch worker
      mockMaster.expectMsg(RegisterNewWorker)
      mockMaster.expectTerminated(worker, 31 seconds)
      workerSystem.shutdown()
    }
  }

  "Worker" should {
    "init its resource from the gearpump config" in {
      val workerSystem = ActorSystem("WorkerSystem", ConfigFactory.parseString(s"${Constants.WORKER_RESOURCE} = {slots = $workerSlots} "))
      val worker = workerSystem.actorOf(Props(classOf[Worker], mockMaster.ref))
      mockMaster watch worker
      mockMaster.expectMsg(RegisterNewWorker)

      worker.tell(WorkerRegistered(workerId), mockMaster.ref)
      mockMaster.expectMsg(ResourceUpdate(workerId, Resource(workerSlots)))

      worker.tell(UpdateResourceFailed("Test resource update failed", new Exception()), mockMaster.ref)
      mockMaster.expectTerminated(worker, 5 seconds)
      workerSystem.shutdown()
    }
  }

  "Worker" should {
    "update its remaining resource when launching and shutting down executors" in {
      val workerSystem = ActorSystem("WorkerSystem", TestUtil.TEST_CONFIG)
      val worker = workerSystem.actorOf(Props(classOf[Worker], masterProxy.ref))
      masterProxy.expectMsg(RegisterNewWorker)

      worker.tell(WorkerRegistered(workerId), mockMaster.ref)
      mockMaster.expectMsg(ResourceUpdate(workerId, Resource(100)))

      val executorName = ActorUtil.actorNameForExecutor(appId, executorId)
      val reportBack = "dummy"   //This is an actor path which the ActorSystemBooter will report back to, not needed in this test.
      val executionContext = ExecutorContext(Util.getCurrentClassPath, workerSystem.settings.config.getString(Constants.GEARPUMP_APPMASTER_ARGS).split(" "), classOf[ActorSystemBooter].getName, Array(executorName, reportBack), None, username = "user")

      //Test LaunchExecutor
      worker.tell(LaunchExecutor(appId, executorId, Resource(101), executionContext), mockMaster.ref)
      mockMaster.expectMsg(ExecutorLaunchRejected("There is no free resource on this machine", Resource(101)))

      worker.tell(LaunchExecutor(appId, executorId, Resource(5), executionContext), mockMaster.ref)
      mockMaster.expectMsg(ResourceUpdate(workerId, Resource(95)))

      //Test terminationWatch
      worker ! ShutdownExecutor(appId, executorId, "Test shut down executor")
      mockMaster.expectMsg(ResourceUpdate(workerId, Resource(100)))

      mockMaster.ref ! PoisonPill
      masterProxy.expectMsg(RegisterWorker(workerId))
      workerSystem.shutdown()
    }
  }
}
