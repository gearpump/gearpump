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

import akka.actor.{DeadLetter, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.gearpump.experiments.yarn.master.AmActorProtocol._
import org.apache.gearpump.experiments.yarn.master.{YarnClusterStats, ResourceManagerCallbackHandler, StopSystemAfterAll}
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records.{Container, FinalApplicationStatus, Resource}
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.mockito.ArgumentMatcher
import org.scalatest.{Assertions, BeforeAndAfter, FlatSpecLike}
import org.specs2.mock.Mockito

import scala.collection.JavaConversions._


class ResourceManagerClientSpec extends TestKit(ActorSystem("testsystem"))
with FlatSpecLike
with StopSystemAfterAll
with Mockito
with Assertions
with ImplicitSender
with TestConfiguration {

  private def getResourceManagerClient(yarnConfig: YarnConfiguration, appConfig: AppConfig,
                                       create: (AppConfig, ActorRef) => ResourceManagerCallbackHandler,
                                       start: (ResourceManagerCallbackHandler) => AMRMClientAsync[ContainerRequest]): TestActorRef[ResourceManagerClient] = {
    val actorProps = Props(classOf[ResourceManagerClient], yarnConfiguration, appConfig, create, start)

    TestActorRef[ResourceManagerClient](actorProps)
  }

  private def getResourceManagerClientWithMockedParent(yarnConfig: YarnConfiguration, appConfig: AppConfig,
                                                       create: (AppConfig, ActorRef) => ResourceManagerCallbackHandler,
                                                       start: (ResourceManagerCallbackHandler) => AMRMClientAsync[ContainerRequest],
                                                       parent: ActorRef): TestActorRef[ResourceManagerClient] = {
    val actorProps = Props(classOf[ResourceManagerClient], yarnConfiguration, appConfig, create, start)
    TestActorRef[ResourceManagerClient](actorProps, parent, "ChildActor")
  }


  case class ContainerRequestEqualityArgumentMatcher(thisObject: ContainerRequest) extends ArgumentMatcher[ContainerRequest] {
    override def matches(arg: Object): Boolean = {
      val o2 = arg.asInstanceOf[ContainerRequest]
      thisObject.getCapability.getMemory == o2.getCapability.getMemory &&
        thisObject.getCapability.getVirtualCores == o2.getCapability.getVirtualCores
    }
  }

  "A ResourceManagerClient" should "start AMRMClientAsync and respond with RMConnected to sender when AMRMClientAsync starts" in {
    val probe = TestProbe()
    val mockedClient = mock[AMRMClientAsync[ContainerRequest]]
    val callbackHandler = mock[ResourceManagerCallbackHandler]
    callbackHandler.resourceManagerClient returns(probe.ref)
    getResourceManagerClient(yarnConfiguration, appConfig, (_,_) => callbackHandler, (callbackHandler) => mockedClient)
    probe.expectMsg(RMConnected)
  }

  it should "respond with RMConnectionFailed(throwable) to sender when AMRMClientAsync cannot be started" in {
    val probe = TestProbe()
    val erroneousClient = mock[AMRMClientAsync[ContainerRequest]]
    val clientException = new RuntimeException
    val callbackHandler = mock[ResourceManagerCallbackHandler]
    callbackHandler.resourceManagerClient returns(probe.ref)
    erroneousClient.start() throws clientException
    getResourceManagerClient(yarnConfiguration, appConfig, (_,_) => callbackHandler, (callbackHandler) => {
      erroneousClient.start()
      erroneousClient
    })
    probe.expectMsg(RMConnectionFailed(clientException))
  }

  it should "call AMRMClientAsync.addContainerRequest when receiving ContainersRequest" in {
    val mockedClient = mock[AMRMClientAsync[ContainerRequest]]
    val client = getResourceManagerClient(yarnConfiguration, appConfig, (_, _) => getMockedCallbackHandler, (_) => mockedClient)
    val req = ContainersRequest(List[Resource](Resource.newInstance(1024, 1)))
    client ! req
    org.mockito.Mockito.verify(mockedClient).addContainerRequest(org.mockito.Matchers.argThat(
      ContainerRequestEqualityArgumentMatcher(client.underlyingActor.createContainerRequest(req.containers.head))))
  }

  it should "call AMRMClientAsync.registerApplicationMaster and respond with RegisterAppMasterResponse when receiving RegisterAMMessage" in {
    val mockedClient = mock[AMRMClientAsync[ContainerRequest]]
    val expectedResponse = mock[RegisterApplicationMasterResponse]
    val masterUrl = "masterUrl"
    val client = getResourceManagerClient(yarnConfiguration, appConfig, (_, _) => getMockedCallbackHandler, (_) => mockedClient)
    mockedClient.registerApplicationMaster(TEST_MASTER_HOSTNAME, TEST_MASTER_PORT.toInt, masterUrl) returns expectedResponse
    client ! RegisterAMMessage(TEST_MASTER_HOSTNAME, TEST_MASTER_PORT.toInt, masterUrl)
    one(mockedClient).registerApplicationMaster(TEST_MASTER_HOSTNAME, TEST_MASTER_PORT.toInt, masterUrl)
    expectMsg(RegisterAppMasterResponse(expectedResponse))
  }


  it should "call AMRMClientAsync.unregisterApplicationMaster and respond with RMError when receiving RMError" in {
    val probe = TestProbe()
    val mockedClient = mock[AMRMClientAsync[ContainerRequest]]
    val client = getResourceManagerClientWithMockedParent(yarnConfiguration, appConfig, (_, _) => getMockedCallbackHandler, (_) => mockedClient, probe.ref)
    val message = "junit"
    val rmError = RMError(new RuntimeException(message))
    client ! rmError
    one(mockedClient).unregisterApplicationMaster(FinalApplicationStatus.FAILED, message, null)
    probe.expectMsg(rmError)
  }

  it should "send RMConnected to parent when receiving RMConnected" in {
    val probe = TestProbe()
    val mockedClient = mock[AMRMClientAsync[ContainerRequest]]
    val client = getResourceManagerClientWithMockedParent(yarnConfiguration, appConfig, (_, _) => getMockedCallbackHandler, (_) => mockedClient, probe.ref)
    client ! RMConnected
    probe.expectMsg(RMConnected)
  }

  it should "send RMConnectionFailed to parent when receiving RMConnectionFailed" in {
    val probe = TestProbe()
    val mockedClient = mock[AMRMClientAsync[ContainerRequest]]
    val client = getResourceManagerClientWithMockedParent(yarnConfiguration, appConfig, (_, _) => getMockedCallbackHandler, (_) => mockedClient, probe.ref)
    val failed = RMConnectionFailed(new RuntimeException)
    client ! failed
    probe.expectMsg(failed)
  }

  it should "send ContainersAllocated to parent when receiving ContainersAllocated" in {
    val probe = TestProbe()
    val mockedClient = mock[AMRMClientAsync[ContainerRequest]]
    val client = getResourceManagerClientWithMockedParent(yarnConfiguration, appConfig, (_, _) => getMockedCallbackHandler, (_) => mockedClient, probe.ref)
    val msg = ContainersAllocated(List.empty)
    client ! msg
    probe.expectMsg(msg)
  }

  it should "send ContainersCompleted to parent when receiving ContainersCompleted" in {
    val probe = TestProbe()
    val mockedClient = mock[AMRMClientAsync[ContainerRequest]]
    val client = getResourceManagerClientWithMockedParent(yarnConfiguration, appConfig, (_, _) => getMockedCallbackHandler, (_) => mockedClient, probe.ref)
    val msg = ContainersCompleted(List.empty)
    client ! msg
    probe.expectMsg(msg)
  }

  it should "stop AMRMClient, unregister AppMaster and send ResourceManagerClient to parent when receiving RMShutdownRequest" in {
    val probe = TestProbe()
    val mockedClient = mock[AMRMClientAsync[ContainerRequest]]
    val client = getResourceManagerClientWithMockedParent(yarnConfiguration, appConfig, (_, _) => getMockedCallbackHandler, (_) => mockedClient, probe.ref)
    client ! RMShutdownRequest
    one(mockedClient).stop
    one(mockedClient).unregisterApplicationMaster(FinalApplicationStatus.KILLED, "Killed", null)
    probe.expectMsg(RMShutdownRequest)
  }

  it should "stop unregister AppMaster and send PoisonPill to itself when receiving RMShutdownRequest" in {
    val probe = TestProbe()
    val mockedClient = mock[AMRMClientAsync[ContainerRequest]]
    val client = getResourceManagerClientWithMockedParent(yarnConfiguration, appConfig, (_, _) => getMockedCallbackHandler, (_) => mockedClient, probe.ref)
    client ! AMShutdownRequest("stats")
    one(mockedClient).unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "stats", null)
    probe watch client
    probe.expectTerminated(client)
  }

  private def getMockedCallbackHandler: ResourceManagerCallbackHandler = {
    val callbackHandler = mock[ResourceManagerCallbackHandler]
    callbackHandler.resourceManagerClient returns(TestActorRef[ResourceManagerClient](Props.empty))
    callbackHandler
  }
}
