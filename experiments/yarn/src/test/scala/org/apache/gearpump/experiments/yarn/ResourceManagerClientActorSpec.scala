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
import org.apache.gearpump.experiments.yarn.master.AmActorProtocol._
import org.apache.gearpump.experiments.yarn.master.{ResourceManagerCallbackHandler, StopSystemAfterAll}
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.mockito.ArgumentCaptor
import org.scalatest.Matchers._
import org.scalatest.{Assertions, BeforeAndAfter, FlatSpecLike}
import org.specs2.mock.Mockito

import scala.util.{Failure, Try}

class ResourceManagerClientActorSpec extends TestKit(ActorSystem("testsystem"))
with FlatSpecLike
with StopSystemAfterAll
with BeforeAndAfter
with ImplicitSender
with Mockito
with Assertions
with TestConfiguration {

  var probe: TestProbe = _

  before {
    probe = TestProbe()
  }

  private def getResourceManagerClientActor(yarnConf: YarnConfiguration, clientFactory: AMRMClientAsyncFactory[ContainerRequest]): TestActorRef[ResourceManagerClientActor] = {
    val actorProps = Props(new ResourceManagerClientActor(yarnConfiguration, clientFactory))
    TestActorRef[ResourceManagerClientActor](actorProps)
  }

  private def getMockedClientFactory(mockedClient: AMRMClientAsync[ContainerRequest]): AMRMClientAsyncFactory[ContainerRequest] = {
    new AMRMClientAsyncFactory[ContainerRequest] {
      override def newIntance(intervalMs: Int, callbackHandler: AMRMClientAsync.CallbackHandler): AMRMClientAsync[ContainerRequest] = {
        mockedClient
      }
    }
  }
  private def getEmptyClientFactory: AMRMClientAsyncFactory[ContainerRequest] = mock[AMRMClientAsyncFactory[ContainerRequest]]

  "A ResourceManagerClientActor" should "start AMRMClientAsync and respond with AMRMClientAsyncStartup(Try(true)) to sender when receiving ResourceManagerCallbackHandler" in {
    val mockedClient = mock[AMRMClientAsync[ContainerRequest]]
    val clientActor = getResourceManagerClientActor(yarnConfiguration, getMockedClientFactory(mockedClient))
    clientActor ! mock[ResourceManagerCallbackHandler]
    clientActor.underlyingActor.client shouldBe an [AMRMClientAsync[ContainerRequest]]
    one(mockedClient).start()
    expectMsg(AMRMClientAsyncStartup(Try(true)))
    expectNoMsg()
  }

  "A ResourceManagerClientActor" should "respond with AMRMClientAsyncStartup(Failure(throwable)) to sender when receiving ResourceManagerCallbackHandler and AMRMClientAsync cannot be started" in {
    val erroneousClient = mock[AMRMClientAsync[ContainerRequest]]
    val clientException = new RuntimeException
    erroneousClient.start() throws clientException
    val badInitializedActor = getResourceManagerClientActor(yarnConfiguration, getMockedClientFactory(erroneousClient))
    badInitializedActor ! mock[ResourceManagerCallbackHandler]
    expectMsg(AMRMClientAsyncStartup(Failure(clientException)))
    expectNoMsg()
  }

  "A ResourceManagerClientActor" should "call AMRMClientAsync.addContainerRequest when receiving ContainerRequestMessage" in {
    val clientActor = getResourceManagerClientActor(yarnConfiguration, getEmptyClientFactory)
    val req = ContainerRequestMessage(1024, 1)
    val clientSpy = mock[AMRMClientAsync[ContainerRequest]]
    val containerRequest = clientActor.underlyingActor.createContainerRequest(req)
    clientActor.underlyingActor.client = clientSpy
    clientActor ! req
    val captor = ArgumentCaptor.forClass(classOf[ContainerRequest])
    one(clientSpy).addContainerRequest(captor.capture())

    withClue("ResourceManagerClientActor generated unexpected ContainerMessage : ") {
      captor.getValue.toString shouldBe containerRequest.toString
    }
  }

  "A ResourceManagerClientActor" should "call AMRMClientAsync.registerApplicationMaster and respond with RegisterAppMasterResponse when receiving RegisterAMMessage" in {
    val mockedClient = mock[AMRMClientAsync[ContainerRequest]]
    val expectedResponse = mock[RegisterApplicationMasterResponse]
    val masterUrl = "masterUrl"
    val clientActor = getResourceManagerClientActor(yarnConfiguration, getEmptyClientFactory)
    mockedClient.registerApplicationMaster(TEST_MASTER_HOSTNAME, TEST_MASTER_PORT.toInt, masterUrl) returns expectedResponse
    clientActor.underlyingActor.client = mockedClient
    clientActor ! RegisterAMMessage(TEST_MASTER_HOSTNAME, TEST_MASTER_PORT.toInt, masterUrl)
    one(mockedClient).registerApplicationMaster(TEST_MASTER_HOSTNAME, TEST_MASTER_PORT.toInt, masterUrl)
    expectMsg(RegisterAppMasterResponse(expectedResponse))
    expectNoMsg()
  }

  "A ResourceManagerClientActor" should "call AMRMClientAsync.unregisterApplicationMaster when receiving AMStatusMessage" in {
    val mockedClient = mock[AMRMClientAsync[ContainerRequest]]
    val clientActor = getResourceManagerClientActor(yarnConfiguration, getEmptyClientFactory)
    clientActor.underlyingActor.client = mockedClient
    val statusMsg = mock[AMStatusMessage]
    val appMessage = "appMessage"
    val appTrackingTool = "appTrackingTool"
    statusMsg.appStatus returns FinalApplicationStatus.SUCCEEDED
    statusMsg.appMessage returns appMessage
    statusMsg.appTrackingUrl returns appTrackingTool
    clientActor ! statusMsg
    one(mockedClient).unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, appMessage, appTrackingTool)
  }
}
