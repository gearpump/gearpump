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

import akka.actor.{Props, ActorSystem}
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class AppManagerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll  {

  def this() = this(ActorSystem("AppManagerSpec"))

  val mockMaster = TestProbe()

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "AppManager" should {
    "do" in {
      val workerSystem = ActorSystem("Master", TestUtil.DEFAULT_CONFIG)
      val appManager = workerSystem.actorOf(Props[AppManager], classOf[AppManager].getSimpleName)
      Thread.sleep(3000)
    }
  }

}
