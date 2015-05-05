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

package org.apache.gearpump.experiments.yarn.master

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import org.apache.gearpump.experiments.yarn.AppConfig
import org.scalatest.FlatSpecLike
import org.specs2.mock.Mockito

class ResourceManagerCallbackHandlerActorSpec extends TestKit(ActorSystem("testsystem"))
with FlatSpecLike
with StopSystemAfterAll
with Mockito {

  "An ResourceManagerCallbackHandlerActor" should "send ResourceManagerCallbackHandler to AmActor after creation" in {
    val probe = TestProbe()
    val handlerActor = getResourceManagerCallbackHandlerActor(mock[AppConfig], probe.ref)
    probe.expectMsg(handlerActor.underlyingActor.rmCallbackHandler)
  }

  private def getResourceManagerCallbackHandlerActor(appConfig:AppConfig, callbackActorRef: ActorRef): TestActorRef[ResourceManagerCallbackHandlerActor] = {
    val actorProps = Props(new ResourceManagerCallbackHandlerActor(appConfig, callbackActorRef))
    TestActorRef[ResourceManagerCallbackHandlerActor](actorProps)
  }
}
