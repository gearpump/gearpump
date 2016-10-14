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

import akka.actor.Props
import akka.testkit.TestProbe
import com.typesafe.config.Config
import org.apache.gearpump.cluster.master.InMemoryKVService
import org.apache.gearpump.cluster.master.InMemoryKVService._
import org.apache.gearpump.cluster.{MasterHarness, TestUtil}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.concurrent.duration._

class InMemoryKVServiceSpec
  extends FlatSpec with Matchers with BeforeAndAfterEach with MasterHarness {

  override def beforeEach(): Unit = {
    startActorSystem()
  }

  override def afterEach(): Unit = {
    shutdownActorSystem()
  }

  override def config: Config = TestUtil.MASTER_CONFIG

  "KVService" should "get, put, delete correctly" in {
    val system = getActorSystem
    val kvService = system.actorOf(Props(new InMemoryKVService()))
    val group = "group"

    val client = TestProbe()(system)

    client.send(kvService, PutKV(group, "key", 1))
    client.expectMsg(PutKVSuccess)

    client.send(kvService, PutKV(group, "key", 2))
    client.expectMsg(PutKVSuccess)

    client.send(kvService, GetKV(group, "key"))
    client.expectMsg(GetKVSuccess("key", 2))

    client.send(kvService, DeleteKVGroup(group))

    // After DeleteGroup, it no longer accept Get and Put message for this group.
    client.send(kvService, GetKV(group, "key"))
    client.expectNoMsg(3.seconds)

    client.send(kvService, PutKV(group, "key", 3))
    client.expectNoMsg(3.seconds)
  }
}
