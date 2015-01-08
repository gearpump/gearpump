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

package org.apache.gearpump.cluster.master

import akka.actor.Props
import akka.testkit.TestProbe
import org.apache.gearpump.cluster.master.InMemoryKVService._
import org.apache.gearpump.cluster.master.MasterHAService.{DeleteMasterStateSuccess, UpdateMasterStateSuccess, UpdateMasterState, MasterState}
import org.apache.gearpump.cluster.{MasterHarness, TestUtil}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class MasterHAServiceSpec extends FlatSpec with Matchers with BeforeAndAfterEach with MasterHarness {

  override def beforeEach() = {
    startActorSystem()
  }

  override def afterEach() = {
    shutdownActorSystem()
  }

  override def config = TestUtil.MASTER_CONFIG

  "HAService" should "get, put, delete correctly" in {
    val system = getActorSystem
    val ha = system.actorOf(Props(new MasterHAService()))
    val group = "group"

    val client = TestProbe()(system)

    client.send(ha, MasterHAService.GetMasterState)
    client.expectMsg(MasterState(0, Set.empty[ApplicationState]))

    val appIdA = 0
    val appIdB = 1
    val stateA = ApplicationState(appIdA, 0, null, null, null, "A")
    val stateB = ApplicationState(appIdB, 0, null, null, null, "B")

    client.send(ha, UpdateMasterState(stateA))
    client.expectMsg(UpdateMasterStateSuccess)

    client.send(ha, UpdateMasterState(stateB))
    client.expectMsg(UpdateMasterStateSuccess)

    client.send(ha, MasterHAService.GetMasterState)
    client.expectMsg(MasterState(Math.max(appIdA, appIdB), Set(stateA, stateB)))

    client.send(ha, MasterHAService.DeleteMasterState(appIdB))
    client.expectMsg(DeleteMasterStateSuccess)

    client.send(ha, MasterHAService.GetMasterState)

    //We still keep the history of max AppId
    client.expectMsg(MasterState(Math.max(appIdA, appIdB), Set(stateA)))
  }
}
