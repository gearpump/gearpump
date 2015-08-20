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

package io.gearpump.util

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.TestProbe
import io.gearpump.cluster.TestUtil
import io.gearpump.util.ActorSystemBooter.{ActorCreated, RegisterActorSystem}
import io.gearpump.cluster.TestUtil
import io.gearpump.util.ActorSystemBooter._
import io.gearpump.util.ActorSystemBooterSpec._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

class ActorSystemBooterSpec extends FlatSpec with Matchers with MockitoSugar {

  "ActorSystemBooter" should "report its address back" in {
    val boot = bootSystem()
    boot.prob.expectMsgType[RegisterActorSystem]
    boot.shutdown
  }

  "ActorSystemBooter" should "terminate itself when parent actor dies" in {
    val boot = bootSystem()
    boot.prob.expectMsgType[RegisterActorSystem]

    val dummy = boot.host.actorOf(Props(classOf[Dummy]), "dummy")
    boot.prob.reply(ActorSystemRegistered(boot.prob.ref))
    boot.prob.reply(BindLifeCycle(dummy))
    boot.host.stop(dummy)
    val terminated = retry(5)(boot.bootedSystem.isTerminated)
    assert(terminated)
    boot.shutdown
  }

  "ActorSystemBooter" should "create new actor" in {
    val boot = bootSystem()
    boot.prob.expectMsgType[RegisterActorSystem]
    boot.prob.reply(ActorSystemRegistered(boot.prob.ref))
    boot.prob.reply(CreateActor(Props(classOf[AcceptThreeArguments], 1, 2, 3), "three"))
    boot.prob.expectMsgType[ActorCreated]

    boot.prob.reply(CreateActor(Props(classOf[AcceptZeroArguments]), "zero"))
    boot.prob.expectMsgType[ActorCreated]

    boot.shutdown
  }

  private def bootSystem() : Boot = {
    val booter = ActorSystemBooter(TestUtil.DEFAULT_CONFIG)

    val system = ActorSystem("reportback", TestUtil.DEFAULT_CONFIG)

    val receiver = TestProbe()(system)
    val address = ActorUtil.getFullPath(system, receiver.ref.path)

    val bootSystem = booter.boot("booter", address)

    Boot(system, receiver, bootSystem)
  }

  case class Boot(host : ActorSystem, prob : TestProbe, bootedSystem : ActorSystem) {
    def shutdown = {
      host.shutdown()
      bootedSystem.shutdown()
    }
  }

  def retry(seconds: Int)(fn: => Boolean) : Boolean = {
    val result = fn
    if (result) {
      result
    } else {
      Thread.sleep(1000)
      retry(seconds - 1)(fn)
    }
  }
}

object ActorSystemBooterSpec {
  class Dummy extends Actor {
    def receive : Receive = {
      case _ =>
    }
  }

  class AcceptZeroArguments extends Actor {
    def receive : Receive = {
      case _ =>
    }
  }

  class AcceptThreeArguments(a : Int, b : Int, c : Int) extends Actor {
    def receive : Receive = {
      case _ =>
    }
  }
}