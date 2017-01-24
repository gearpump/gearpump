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
package org.apache.gearpump.experiments.distributeservice

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestProbe}
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import org.apache.gearpump.cluster.AppMasterToMaster.{GetAllWorkers, RegisterAppMaster, RequestResource}
import org.apache.gearpump.cluster.AppMasterToWorker.LaunchExecutor
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterRegistered, ResourceAllocated, WorkerList}
import org.apache.gearpump.cluster.appmaster.{AppMasterRuntimeEnvironment, ApplicationRuntimeInfo}
import org.apache.gearpump.cluster.scheduler.{Relaxation, Resource, ResourceAllocation, ResourceRequest}
import org.apache.gearpump.cluster.worker.WorkerId
import org.apache.gearpump.cluster.{AppDescription, AppMasterContext, TestUtil, UserConfig}
import org.apache.gearpump.experiments.distributeservice.DistServiceAppMaster.{FileContainer, GetFileContainer}
import org.apache.gearpump.util.ActorSystemBooter.RegisterActorSystem
import org.apache.gearpump.util.ActorUtil

class DistServiceAppMasterSpec extends WordSpec with Matchers with BeforeAndAfter {
  implicit val system = ActorSystem("AppMasterSpec", TestUtil.DEFAULT_CONFIG)
  val mockMaster = TestProbe()(system)
  val mockWorker1 = TestProbe()(system)
  val client = TestProbe()(system)
  val masterProxy = mockMaster.ref
  val appId = 0
  val userName = "test"
  val masterExecutorId = 0
  val workerList = List(WorkerId(1, 0L), WorkerId(2, 0L), WorkerId(3, 0L))
  val resource = Resource(1)
  val appJar = None
  val appDescription = AppDescription("app0", classOf[DistServiceAppMaster].getName,
    UserConfig.empty)

  "DistService AppMaster" should {
    "responsable for service distributing" in {
      val appMasterContext = AppMasterContext(appId, userName, resource, null, appJar, masterProxy)
      TestActorRef[DistServiceAppMaster](
        AppMasterRuntimeEnvironment.props(List(masterProxy.path), appDescription,
          appMasterContext))
      val registerAppMaster = mockMaster.receiveOne(15.seconds)
      assert(registerAppMaster.isInstanceOf[RegisterAppMaster])

      val appMaster = registerAppMaster.asInstanceOf[RegisterAppMaster].appMaster
      mockMaster.reply(AppMasterRegistered(appId))
      // The DistributedShell AppMaster will ask for worker list
      mockMaster.expectMsg(GetAllWorkers)
      mockMaster.reply(WorkerList(workerList))
      // After worker list is ready, DistributedShell AppMaster will request resouce on each worker
      workerList.foreach { workerId =>
        mockMaster.expectMsg(RequestResource(appId, ResourceRequest(Resource(1), workerId,
          relaxation = Relaxation.SPECIFICWORKER)))
      }
      mockMaster.reply(ResourceAllocated(Array(ResourceAllocation(resource, mockWorker1.ref,
        WorkerId(1, 0L)))))
      mockWorker1.expectMsgClass(classOf[LaunchExecutor])
      mockWorker1.reply(RegisterActorSystem(ActorUtil.getSystemAddress(system).toString))

      appMaster.tell(GetFileContainer, client.ref)
      client.expectMsgClass(15.seconds, classOf[FileContainer])
    }
  }

  after {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }
}
