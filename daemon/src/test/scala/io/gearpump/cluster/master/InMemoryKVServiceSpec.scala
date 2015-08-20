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

package io.gearpump.cluster.master

import akka.actor.Props
import akka.testkit.TestProbe
import io.gearpump.cluster.master.InMemoryKVService._
import io.gearpump.cluster.{MasterHarness, TestUtil}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class InMemoryKVServiceSpec extends FlatSpec with Matchers with BeforeAndAfterEach with MasterHarness {

  override def beforeEach() = {
    startActorSystem()
  }

  override def afterEach() = {
    shutdownActorSystem()
  }

  override def config = TestUtil.MASTER_CONFIG

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

    client.send(kvService, GetKV(group, "key"))
    client.expectMsg(GetKVSuccess("key", null))
  }
}
