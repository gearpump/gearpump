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

import akka.actor._
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.gearpump.experiments.yarn.TestConfiguration
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfter, FlatSpecLike}
import org.slf4j.Logger

class MockedChild(probe: ActorRef) extends Actor {
  val LOG: Logger = LogUtil.getLogger(getClass)

  override def receive: Receive = {
    case x =>  probe ! x
  }
}

class YarnApplicationMasterSpec extends TestKit(ActorSystem("testsystem"))
with FlatSpecLike
with StopSystemAfterAll
with BeforeAndAfter
with TestConfiguration {
  import AmActorProtocol._

  //warning : one for all tests
  val containerLaunchContextMock = mock[ContainerLaunchContext]
  var nmClientAsyncMock: NMClientAsync = _

  private[this] def createAmActor(probe: TestProbe): TestActorRef[YarnApplicationMaster] = {

    val amActorProps = Props(new YarnApplicationMaster(appConfig,
      yarnConfiguration,
      Props(classOf[MockedChild], probe.ref),
      (config, cb) => nmClientAsyncMock,
      (command) => containerLaunchContextMock
    ))
    TestActorRef[YarnApplicationMaster](amActorProps)
  }

  private def getContainerId: ContainerId = {
    ContainerId.newInstance(ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1), 1)
  }

  private def getContainer: Container = {
    getContainer(NodeId.newInstance(TEST_MASTER_HOSTNAME, 50000))
  }

  private def getContainer(nodeId: NodeId):Container = {
    val token = Token.newInstance(Array[Byte](), "kind",  Array[Byte](), "service")
    Container.newInstance(getContainerId, nodeId, "", Resource.newInstance(1024, 2), Priority.newInstance(1), token)
  }

  private def expectTermination(probe: TestProbe, target: ActorRef): Terminated = {
    probe watch target
    probe.expectTerminated(target)
  }

  before {
    nmClientAsyncMock = mock[NMClientAsync]
  }

  "An AmActor" should "send RegisterAMMessage to ResourceManagerClient when receiving RMConnected" in {
    val msg = RMConnected
    val probe = TestProbe()
    val amActor = createAmActor(probe)
    amActor ! msg
    val port = appConfig.getEnv(YARNAPPMASTER_PORT).toInt
    val host = amActor.underlyingActor.host
    val target = host + ":" + port
    val addr = NetUtils.createSocketAddr(target)
    val trackingURL = amActor.underlyingActor.trackingURL
    probe.expectMsg(new RegisterAMMessage(addr.getHostName, port, trackingURL))
  }

  "An AmActor" should "not send any messages to ResourceManagerClient when receiving RMConnectionFailed(throwable)" in {
    val probe = TestProbe()
    val amActor = createAmActor(probe)
    amActor ! RMConnectionFailed(new RuntimeException)
    probe.expectNoMsg()
  }

  "An AmActor" should "send one or more ContainersRequest to ResourceManagerClient when receiving a RegisterAppMasterResponse" in {
    val response = mock[RegisterApplicationMasterResponse]
    response.getContainersFromPreviousAttempts returns java.util.Collections.emptyList[Container]()
    val msg = RegisterAppMasterResponse(response)
    val probe = TestProbe()
    val amActor = createAmActor(probe)
    amActor ! msg
    probe.expectMsgAllOf(ContainersRequest(appConfig.getEnv(GEARPUMPMASTER_MEMORY).toInt, appConfig.getEnv(GEARPUMPMASTER_VCORES).toInt),
      ContainersRequest(appConfig.getEnv(GEARPUMPMASTER_MEMORY).toInt, appConfig.getEnv(GEARPUMPMASTER_VCORES).toInt))
  }

  "An AmActor" should "stop nodeManagerClient, send AMStatusMessage to ResourceManagerClient with Succeed status and stop it when receiving RMAllRequestedContainersCompleted" in {
    val stats = ContainerStats(3, 3, 0)
    val probe = TestProbe()
    val amActor = createAmActor(probe)
    amActor ! RMAllRequestedContainersCompleted(stats)
    one(nmClientAsyncMock).stop()
    noMoreCallsTo(nmClientAsyncMock)
    expectTermination(probe, amActor.underlyingActor.resourceManagerClient)
  }

  "An AmActor" should "stop nodeManagerClient, send AMStatusMessage to ResourceManagerClient with Failed status and stop it when receiving RMError" in {
    val stats = ContainerStats(3, 0, 3)
    val probe = TestProbe()
    val amActor = createAmActor(probe)
    amActor ! RMError(new RuntimeException, stats)
    one(nmClientAsyncMock).stop()
    noMoreCallsTo(nmClientAsyncMock)
    expectTermination(probe, amActor.underlyingActor.resourceManagerClient)
  }

  "An AmActor" should "stop nodeManagerClient, send AMStatusMessage to ResourceManagerClient with Killed status and stop it when receiving RMShutdownRequest" in {
    val stats = ContainerStats(3, 3, 0)
    val probe = TestProbe()
    val amActor = createAmActor(probe)
    amActor ! RMShutdownRequest(stats)
    one(nmClientAsyncMock).stop()
    noMoreCallsTo(nmClientAsyncMock)
    expectTermination(probe, amActor.underlyingActor.resourceManagerClient)
  }

  "An AmActor" should "stop nodeManagerClient, send AMStatusMessage to ResourceManagerClient with Failed status and stop it when receiving RMShutdownRequest and any containers failed" in {
    val stats = ContainerStats(3, 0, 3)
    val probe = TestProbe()
    val amActor = createAmActor(probe)
    amActor ! RMShutdownRequest(stats)
    one(nmClientAsyncMock).stop()
    noMoreCallsTo(nmClientAsyncMock)
    expectTermination(probe, amActor.underlyingActor.resourceManagerClient)
  }

  "An AmActor" should  "send ContainersRequest to ResourceManagerClient when AdditionalContainersRequest is sent to it" in {
    val probe = TestProbe()
    val amActor = createAmActor(probe)
    amActor ! AdditionalContainersRequest(1)
    probe.expectMsg(ContainersRequest(1024, 2))
  }

  "An AmActor with 0 masters started" should "set masterAddr and start container(s) for master(s) using nodeManagerClient when receiving ContainersAllocated" in {
    val probe = TestProbe()
    val amActor = createAmActor(probe)
    amActor.underlyingActor.masterContainersStarted = 0
    amActor.underlyingActor.masterAddr = None
    val container1 = getContainer(NodeId.newInstance("host1", 3001))
    val container2 = getContainer(NodeId.newInstance("host2", 3002))
    amActor ! ContainersAllocated(List(container1, container2))
    amActor.underlyingActor.masterAddr.get shouldBe HostPort("host1:3001")
    one(nmClientAsyncMock).startContainerAsync(container1, containerLaunchContextMock)
    one(nmClientAsyncMock).startContainerAsync(container2, containerLaunchContextMock)
    noMoreCallsTo(nmClientAsyncMock)
    // TODO this should show assertion error but it doesn't work (probably some traits related problem)
    // there was one(nmClientAsyncMock).startContainerAsync(container2, containerLaunchContextMock)
  }

  "An AmActor with enough masters started" should "inc workerContainerRequested and start container(s) for workers(s) using nodeManagerClient when receiving ContainersAllocated" in {
    val probe = TestProbe()
    val amActor = createAmActor(probe)
    amActor.underlyingActor.masterContainersStarted = TEST_MASTER_CONTAINERS
    amActor.underlyingActor.workerContainersStarted = 0
    amActor.underlyingActor.workerContainersRequested = 0
    amActor.underlyingActor.masterAddr = Some(HostPort(s"$TEST_MASTER_HOSTNAME:$TEST_MASTER_PORT"))
    amActor ! ContainersAllocated(List(getContainer, getContainer, getContainer))
    amActor.underlyingActor.workerContainersRequested shouldBe 3
    3.times(nmClientAsyncMock).startContainerAsync(getContainer, containerLaunchContextMock)
    noMoreCallsTo(nmClientAsyncMock)
  }


  "An AmActor without enough masters started" should "only increment started masters counter when ContainerStarted is send to it" in {
    val probe = TestProbe()
    val amActor = createAmActor(probe)
    amActor.underlyingActor.masterContainersStarted = 0
    amActor ! ContainerStarted(getContainerId)
    amActor.underlyingActor.masterContainersStarted shouldBe 1
  }

  "An AmActor that just started enough masters" should "increment started masters counter and request all worker containers when ContainerStarted is send to it" in {
    val probe = TestProbe()
    val amActor = createAmActor(probe)
    amActor.underlyingActor.masterContainersStarted = TEST_MASTER_CONTAINERS - 1
    amActor.underlyingActor.workerContainersStarted = 0
    amActor ! ContainerStarted(getContainerId)
    amActor.underlyingActor.masterContainersStarted shouldBe TEST_MASTER_CONTAINERS
    //TODO: Number of ContainerRequestMessage should depends on WORKER_CONTAINER_COUNT
    probe.expectMsgAllOf(ContainersRequest(appConfig.getEnv(WORKER_MEMORY).toInt, appConfig.getEnv(WORKER_VCORES).toInt),
      ContainersRequest(appConfig.getEnv(WORKER_MEMORY).toInt, appConfig.getEnv(WORKER_VCORES).toInt),
      ContainersRequest(appConfig.getEnv(WORKER_MEMORY).toInt, appConfig.getEnv(WORKER_VCORES).toInt))
  }

  "An AmActor that started enough masters but not enough workers" should "only increment started worker counter when ContainerStarted is send to it" in {
    val probe = TestProbe()
    val amActor = createAmActor(probe)
    amActor.underlyingActor.masterContainersStarted = TEST_MASTER_CONTAINERS
    amActor.underlyingActor.workerContainersStarted = TEST_WORKER_CONTAINERS - 2
    amActor ! ContainerStarted(getContainerId)
    amActor.underlyingActor.masterContainersStarted shouldBe TEST_MASTER_CONTAINERS
  }

  "An AmActor that started enough workers" should "start services actor" in {
    //how to test that?
  }

}
