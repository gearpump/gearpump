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

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.gearpump.experiments.yarn.master.AmActorProtocol._
import org.apache.gearpump.experiments.yarn.master.{ResourceManagerCallbackHandler, StopSystemAfterAll}
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records.{Container, FinalApplicationStatus}
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
with BeforeAndAfter
with ImplicitSender
with Mockito
with Assertions
with TestConfiguration {

  private def getResourceManagerClient(yarnConfig: YarnConfiguration, appConfig: AppConfig,
                                       create: (AppConfig, ActorRef) => ResourceManagerCallbackHandler,
                                       start: (ResourceManagerCallbackHandler) => AMRMClientAsync[ContainerRequest]): TestActorRef[ResourceManagerClient] = {
    val actorProps = Props(classOf[ResourceManagerClient], yarnConfiguration, appConfig, Some(create), Some(start))
    TestActorRef[ResourceManagerClient](actorProps)
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
    val callBackHandler = new ResourceManagerCallbackHandler(appConfig, probe.ref)
    val client = getResourceManagerClient(yarnConfiguration, appConfig, (appConfig, _) => callBackHandler, (callbackHandler) => mockedClient)
    probe.expectMsg(RMConnected)
  }

  "A ResourceManagerClient" should "respond with RMConnectionFailed(throwable) to sender when AMRMClientAsync cannot be started" in {
    val probe = TestProbe()
    val erroneousClient = mock[AMRMClientAsync[ContainerRequest]]
    val clientException = new RuntimeException
    val callBackHandler = new ResourceManagerCallbackHandler(appConfig, probe.ref)
    erroneousClient.start() throws clientException
    getResourceManagerClient(yarnConfiguration, appConfig, (appConfig, _) => callBackHandler, (callbackHandler) => {
      erroneousClient.start()
      erroneousClient
    })
    probe.expectMsg(RMConnectionFailed(clientException))
  }

  "A ResourceManagerClient" should "receive an ContainersAllocated when ResourceManagerCallbackHandler.onContainersAllocated is called" in {
    val probe = TestProbe()
    val callBackHandler = new ResourceManagerCallbackHandler(appConfig, probe.ref)
    val containers = List.empty[Container]
    callBackHandler.onContainersAllocated(containers)
    probe.expectMsg(ContainersAllocated(containers))
  }

  "A ResourceManagerClient" should "call AMRMClientAsync.addContainerRequest when receiving ContainersRequest" in {
    val probe = TestProbe()
    val mockedClient = mock[AMRMClientAsync[ContainerRequest]]
    val callBackHandler = new ResourceManagerCallbackHandler(appConfig, probe.ref)
    val client = getResourceManagerClient(yarnConfiguration, appConfig, (_, _) => callBackHandler, (_) => mockedClient)
    val req = ContainersRequest(1024, 1)
    client ! req
    org.mockito.Mockito.verify(mockedClient).addContainerRequest(org.mockito.Matchers.argThat(
      ContainerRequestEqualityArgumentMatcher(client.underlyingActor.createContainerRequest(req))))
  }

  "A ResourceManagerClient" should "call AMRMClientAsync.registerApplicationMaster and respond with RegisterAppMasterResponse when receiving RegisterAMMessage" in {
    val probe = TestProbe()
    val mockedClient = mock[AMRMClientAsync[ContainerRequest]]
    val expectedResponse = mock[RegisterApplicationMasterResponse]
    val masterUrl = "masterUrl"
    val callBackHandler = new ResourceManagerCallbackHandler(appConfig, probe.ref)
    val client = getResourceManagerClient(yarnConfiguration, appConfig, (_, _) => callBackHandler, (_) => mockedClient)
    mockedClient.registerApplicationMaster(TEST_MASTER_HOSTNAME, TEST_MASTER_PORT.toInt, masterUrl) returns expectedResponse
    client ! RegisterAMMessage(TEST_MASTER_HOSTNAME, TEST_MASTER_PORT.toInt, masterUrl)
    one(mockedClient).registerApplicationMaster(TEST_MASTER_HOSTNAME, TEST_MASTER_PORT.toInt, masterUrl)
    expectMsg(RegisterAppMasterResponse(expectedResponse))
  }

  "A ResourceManagerClient" should "call AMRMClientAsync.unregisterApplicationMaster when receiving RMError" in {
    val probe = TestProbe()
    val mockedClient = mock[AMRMClientAsync[ContainerRequest]]
    val callBackHandler = new ResourceManagerCallbackHandler(appConfig, probe.ref)
    val client = getResourceManagerClient(yarnConfiguration, appConfig, (_, _) => callBackHandler, (_) => mockedClient)
    val stats = ContainerStats(3, 2, 1)
    val rmError = mock[RMError]
    rmError.throwable returns new RuntimeException
    rmError.containerStats returns stats
    val message = s"Failed. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
    client ! rmError
    one(mockedClient).unregisterApplicationMaster(FinalApplicationStatus.FAILED, message, null)
  }

}
