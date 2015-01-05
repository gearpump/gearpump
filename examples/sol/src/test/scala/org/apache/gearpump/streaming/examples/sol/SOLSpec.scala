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
package org.apache.gearpump.streaming.examples.sol

import akka.actor.{Props, Actor, ActorSystem}
import akka.testkit.TestProbe
import org.apache.gearpump.cluster.TestUtil
import org.scalatest.{Matchers, WordSpec}

import org.apache.gearpump.util.Constants._

class SOLSpec extends WordSpec with Matchers  {

  "SOL" should {
    "be started without exception" in {
      val systemConfig = TestUtil.DEFAULT_CONFIG
      val system = ActorSystem(MASTER, systemConfig)
      val masterReceiver = TestProbe()(system)

      val master = system.actorOf(Props(classOf[MockMaster], masterReceiver), MASTER)
    }
  }
}

class MockMaster(receiver: TestProbe) extends Actor {
  def receive: Receive = {
    case msg => receiver.ref forward msg
  }
}
