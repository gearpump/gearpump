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

package io.gearpump.cluster.appmaster

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.TestProbe
import io.gearpump.cluster.AppMasterToMaster.RegisterAppMaster
import io.gearpump.cluster.appmaster.MasterConnectionKeeper.MasterConnectionStatus.MasterConnected
import io.gearpump.cluster.master.MasterProxy.WatchMaster
import io.gearpump.cluster.AppMasterToMaster.RegisterAppMaster
import io.gearpump.cluster.MasterToAppMaster.AppMasterRegistered
import io.gearpump.cluster.TestUtil
import io.gearpump.cluster.appmaster.MasterConnectionKeeper.MasterConnectionStatus._
import io.gearpump.cluster.appmaster.MasterConnectionKeeperSpec.ConnectionKeeperTestEnv
import io.gearpump.cluster.master.MasterProxy
import io.gearpump.cluster.master.MasterProxy.WatchMaster
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.duration._

class MasterConnectionKeeperSpec  extends FlatSpec with Matchers with BeforeAndAfterAll {

  implicit var system: ActorSystem = null
  val register = RegisterAppMaster(null, null)
  val appId = 0

   override def beforeAll: Unit = {
     system = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
   }

   override def afterAll: Unit = {
    system.shutdown()
  }

  private def startMasterConnectionKeeper: ConnectionKeeperTestEnv = {
    val statusChangeSubscriber = TestProbe()
    val master = TestProbe()

    val keeper = system.actorOf(Props(new MasterConnectionKeeper(register, master.ref, statusChangeSubscriber.ref)))
    statusChangeSubscriber.watch(keeper)

    master.expectMsgType[WatchMaster]

    //master is alive, response to RegisterAppMaster
    master.expectMsgType[RegisterAppMaster]
    master.reply(AppMasterRegistered(appId))

    //notify listener that master is alive
    statusChangeSubscriber.expectMsg(MasterConnected)
    ConnectionKeeperTestEnv(master, keeper, statusChangeSubscriber)
  }

  it should "start correctly and notify listener that master is alive" in {
    startMasterConnectionKeeper
  }

  it should "re-register the appmaster when master is restarted" in {
    import io.gearpump.cluster.master.MasterProxy.MasterRestarted
    val ConnectionKeeperTestEnv(master, keeper, masterChangeListener) = startMasterConnectionKeeper

    //master is restarted
    master.send(keeper, MasterRestarted)
    master.expectMsgType[RegisterAppMaster]
    master.reply(AppMasterRegistered(appId))
    masterChangeListener.expectMsg(MasterConnected)

    //Recovery from Master restart is transparent to listener
    masterChangeListener.expectNoMsg()
  }

  it should "notify listener and then shutdown itself when master is dead" in {
    val ConnectionKeeperTestEnv(master, keeper, masterChangeListener) = startMasterConnectionKeeper

    //master is dead
    master.send(keeper, MasterStopped)

    //keeper should tell the listener that master is stopped before shutting down itself
    masterChangeListener.expectMsg(MasterStopped)
    masterChangeListener.expectTerminated(keeper)
  }

  it should "mark the master as dead when timeout" in {
    val statusChangeSubscriber = TestProbe()
    val master = TestProbe()

    //keeper will register to master by sending RegisterAppMaster
    val keeper = system.actorOf(Props(new MasterConnectionKeeper(register, master.ref, statusChangeSubscriber.ref)))

    //master doesn't reply to keeper,
    statusChangeSubscriber.watch(keeper)

    //timeout, keeper notify listener, and then make suicide
    statusChangeSubscriber.expectMsg(60 seconds, MasterStopped)
    statusChangeSubscriber.expectTerminated(keeper, 60 seconds)
  }
}

object MasterConnectionKeeperSpec {
  case class ConnectionKeeperTestEnv(master: TestProbe, keeper: ActorRef, masterChangeListener: TestProbe)
}
