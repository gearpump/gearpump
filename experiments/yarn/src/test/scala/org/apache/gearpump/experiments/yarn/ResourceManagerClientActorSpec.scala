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

package org.apache.gearpump.experiments.yarn

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.apache.gearpump.experiments.yarn.master.AmActorProtocol.{AMRMClientAsyncStartup, ContainerRequestMessage}
import org.apache.gearpump.experiments.yarn.master.{ResourceManagerCallbackHandler, StopSystemAfterAll}
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfter, FlatSpecLike}
import org.specs2.mock.Mockito

import scala.util.Try

class ResourceManagerClientActorSpec extends TestKit(ActorSystem("testsystem"))
with FlatSpecLike
with StopSystemAfterAll
with BeforeAndAfter
with ImplicitSender
with Mockito {

  val yarnConf = new YarnConfiguration
  var probe: TestProbe = _
  var clientActor: TestActorRef[ResourceManagerClientActor] = _
  before {
    probe = TestProbe()
    clientActor = getResourceManagerClientActor(yarnConf)

  }

  private def getResourceManagerClientActor(yarnConf: YarnConfiguration): TestActorRef[ResourceManagerClientActor] = {
    val actorProps = Props(new ResourceManagerClientActor(yarnConf))
    TestActorRef[ResourceManagerClientActor](actorProps)
  }


  "A ResourceManagerClientActor" should "start AMRMClientAsync and respond with AMRMClientAsyncStartup(Try(true)) to sender when receiving ResourceManagerCallbackHandler" in {
    clientActor ! mock[ResourceManagerCallbackHandler]
    clientActor.underlyingActor.client shouldBe an [AMRMClientAsync[ContainerRequest]]
    expectMsg(AMRMClientAsyncStartup(Try(true)))
    expectNoMsg()
  }

  "A ResourceManagerClientActor" should "respond with AMRMClientAsyncStartup(Failure(throwable)) to sender when receiving ResourceManagerCallbackHandler and AMRMClientAsync cannot be started" in {
    val badInitializedActor = getResourceManagerClientActor(null)
    badInitializedActor ! mock[ResourceManagerCallbackHandler]
    //expectMsg(AMRMClientAsyncStartup(anyObject))
    //expectNoMsg()
  }


  "A ResourceManagerClientActor" should "call AMRMClientAsync.addContainerRequest when receiving ContainerRequestMessage" in {
    val req = ContainerRequestMessage(1024, 1)
    val clientSpy = mock[AMRMClientAsync[ContainerRequest]]
    val containerRequest = clientActor.underlyingActor.createContainerRequest(req)
    clientActor.underlyingActor.client = clientSpy
    clientActor ! req
    //todo:  containerRequest doesn't seems to override equals :/ need to do it some other way (like using toString())
    //one(clientSpy).addContainerRequest(containerRequest)
  }

  "A ResourceManagerClientActor" should "call AMRMClientAsync.registerApplicationMaster and respond with RegisterAppMasterResponse when receiving RegisterAMMessage" in {
  }

  "A ResourceManagerClientActor" should "call AMRMClientAsync.unregisterApplicationMaster when receiving AMStatusMessage" in {
  }


}
