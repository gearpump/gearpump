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

import java.util.concurrent.TimeUnit

import akka.actor.{Terminated, Actor, ActorRef, Props}
import akka.testkit.TestProbe
import org.apache.gearpump.cluster.AppManager.AppMasterLauncher
import org.apache.gearpump.cluster.AppManagerSpec.{WatcheeTerminated, Watcher}
import org.apache.gearpump.cluster.AppMasterToMaster.RequestResource
import org.apache.gearpump.cluster.AppMasterToWorker.LaunchExecutor
import org.apache.gearpump.cluster.MasterToAppMaster.ResourceAllocated
import org.apache.gearpump.cluster.TestUtil.DummyApplication
import org.apache.gearpump.cluster.scheduler.{ResourceAllocation, Resource}
import org.apache.gearpump.util.ActorSystemBooter.{ActorCreated, CreateActor, ActorSystemRegistered, RegisterActorSystem}
import org.scalatest.{BeforeAndAfterEach, Matchers, FlatSpec}

import scala.concurrent.duration.Duration

class AppManagerSpec extends FlatSpec with Matchers with BeforeAndAfterEach with MasterHarness {


  override def beforeEach() = {
    startActorSystem()
  }

  override def afterEach() = {
    shutdownActorSystem()
  }

  "AppMasterLauncher" should "launch appmaster correctly" in {
    val master = createMockMaster()
    val worker = master
    val watcher = master
    val appmaster = master.ref

    val system = getActorSystem
    val launcher = system.actorOf(AppMasterLauncher.props(0,
      new DummyApplication, None, "username", master.ref))
    master.expectMsgType[RequestResource]

    system.actorOf(Props(new Watcher(launcher, watcher)))

    val resource = ResourceAllocated(Array(ResourceAllocation(Resource(1), master.ref, 0)))
    master.reply(resource)

    worker.expectMsgType[LaunchExecutor]
    worker.reply(RegisterActorSystem("systempath"))
    worker.expectMsgType[ActorSystemRegistered]

    worker.expectMsgType[CreateActor]
    worker.reply(ActorCreated(appmaster, "appmaster"))

    watcher.expectMsg(WatcheeTerminated)
  }
}

object AppManagerSpec {

  case object WatcheeTerminated

  class Watcher(target : ActorRef, testProb : TestProbe) extends Actor {
    context.watch(target)
    def receive : Receive = {
      case t : Terminated =>
        testProb.ref forward WatcheeTerminated
    }
  }
}
