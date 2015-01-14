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

package org.apache.gearpump.cluster.master

import akka.actor._
import akka.testkit.TestProbe
import org.apache.gearpump.cluster.AppMasterToMaster.RequestResource
import org.apache.gearpump.cluster.AppMasterToWorker.{ShutdownExecutor, LaunchExecutor}
import org.apache.gearpump.cluster.MasterToClient.SubmitApplicationResult
import org.apache.gearpump.cluster.WorkerToAppMaster.ExecutorLaunchRejected
import org.apache.gearpump.cluster.{TestUtil, MasterHarness}
import org.apache.gearpump.cluster.MasterToAppMaster.ResourceAllocated
import org.apache.gearpump.cluster.scheduler.{ResourceRequest, Resource, ResourceAllocation}
import org.apache.gearpump.util.ActorSystemBooter._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.util.Success

class AppMasterLauncherSpec extends FlatSpec with Matchers with BeforeAndAfterEach with MasterHarness {

  override def config = TestUtil.DEFAULT_CONFIG

  val appId = 1
  val executorId = 2
  var master: TestProbe = null
  var client: TestProbe = null
  var worker: TestProbe = null
  var watcher: TestProbe = null
  var appMasterLauncher: ActorRef = null

  override def beforeEach() = {
    startActorSystem()
    master = createMockMaster()
    client = TestProbe()(getActorSystem)
    worker = TestProbe()(getActorSystem)
    watcher = TestProbe()(getActorSystem)
    appMasterLauncher = getActorSystem.actorOf(AppMasterLauncher.props(appId, executorId,
      TestUtil.dummyApp, None, "username", master.ref, Some(client.ref)))
    watcher watch appMasterLauncher
    master.expectMsg(RequestResource(appId, ResourceRequest(Resource(1))))
    val resource = ResourceAllocated(Array(ResourceAllocation(Resource(1), worker.ref, 0)))
    master.reply(resource)
    worker.expectMsgType[LaunchExecutor]
  }

  override def afterEach() = {
    shutdownActorSystem()
  }

  "AppMasterLauncher" should "launch appmaster correctly" in {
    worker.reply(RegisterActorSystem("systempath"))
    worker.expectMsgType[ActorSystemRegistered]

    worker.expectMsgType[CreateActor]
    worker.reply(ActorCreated(master.ref, "appmaster"))

    client.expectMsg(SubmitApplicationResult(Success(appId)))
    watcher.expectTerminated(appMasterLauncher)
  }

  "AppMasterLauncher" should "reallocate resource if executor launch rejected" in {
    worker.reply(ExecutorLaunchRejected("", Resource(1)))
    master.expectMsg(RequestResource(appId, ResourceRequest(Resource(1))))

    val resource = ResourceAllocated(Array(ResourceAllocation(Resource(1), worker.ref, 0)))
    master.reply(resource)
    worker.expectMsgType[LaunchExecutor]

    worker.reply(RegisterActorSystem("systempath"))
    worker.expectMsgType[ActorSystemRegistered]

    worker.expectMsgType[CreateActor]
    worker.reply(CreateActorFailed("", new Exception))
    worker.expectMsgType[ShutdownExecutor]
    assert(client.receiveN(1).head.asInstanceOf[SubmitApplicationResult].appId.isFailure)
    watcher.expectTerminated(appMasterLauncher)
  }
}
