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

package org.apache.gearpump.cluster.appmaster

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.TestProbe
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import org.apache.gearpump.cluster.AppMasterToMaster.RegisterAppMaster
import org.apache.gearpump.cluster.MasterToAppMaster.AppMasterRegistered
import org.apache.gearpump.cluster.TestUtil
import org.apache.gearpump.cluster.appmaster.MasterConnectionKeeper.MasterConnectionStatus.{MasterConnected, _}
import org.apache.gearpump.cluster.appmaster.MasterConnectionKeeperSpec.ConnectionKeeperTestEnv
import org.apache.gearpump.cluster.master.MasterProxy.WatchMaster

class MasterConnectionKeeperSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  implicit var system: ActorSystem = null
  val appId = 0
  val register = RegisterAppMaster(appId, null, null)

  override def beforeAll(): Unit = {
    system = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
  }

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }

  private def startMasterConnectionKeeper: ConnectionKeeperTestEnv = {
    val statusChangeSubscriber = TestProbe()
    val master = TestProbe()

    val keeper = system.actorOf(Props(
      new MasterConnectionKeeper(register, master.ref, statusChangeSubscriber.ref)))
    statusChangeSubscriber.watch(keeper)

    master.expectMsgType[WatchMaster]

    // Master is alive, response to RegisterAppMaster
    master.expectMsgType[RegisterAppMaster]
    master.reply(AppMasterRegistered(appId))

    // Notify listener that master is alive
    statusChangeSubscriber.expectMsg(MasterConnected)
    ConnectionKeeperTestEnv(master, keeper, statusChangeSubscriber)
  }

  it should "start correctly and notify listener that master is alive" in {
    startMasterConnectionKeeper
  }

  it should "re-register the appmaster when master is restarted" in {
    import org.apache.gearpump.cluster.master.MasterProxy.MasterRestarted
    val ConnectionKeeperTestEnv(master, keeper, masterChangeListener) = startMasterConnectionKeeper

    // Master is restarted
    master.send(keeper, MasterRestarted)
    master.expectMsgType[RegisterAppMaster]
    master.reply(AppMasterRegistered(appId))
    masterChangeListener.expectMsg(MasterConnected)

    // Recovery from Master restart is transparent to listener
    masterChangeListener.expectNoMsg()
  }

  it should "notify listener and then shutdown itself when master is dead" in {
    val ConnectionKeeperTestEnv(master, keeper, masterChangeListener) = startMasterConnectionKeeper

    // Master is dead
    master.send(keeper, MasterStopped)

    // Keeper should tell the listener that master is stopped before shutting down itself
    masterChangeListener.expectMsg(MasterStopped)
    masterChangeListener.expectTerminated(keeper)
  }

  it should "mark the master as dead when timeout" in {
    val statusChangeSubscriber = TestProbe()
    val master = TestProbe()

    // MasterConnectionKeeper register itself to master by sending RegisterAppMaster
    val keeper = system.actorOf(Props(new MasterConnectionKeeper(register,
      master.ref, statusChangeSubscriber.ref)))

    // Master doesn't reply to keeper,
    statusChangeSubscriber.watch(keeper)

    // Timeout, keeper notify listener, and then make suicide
    statusChangeSubscriber.expectMsg(60.seconds, MasterStopped)
    statusChangeSubscriber.expectTerminated(keeper, 60.seconds)
  }
}

object MasterConnectionKeeperSpec {
  case class ConnectionKeeperTestEnv(
      master: TestProbe, keeper: ActorRef, masterChangeListener: TestProbe)
}
