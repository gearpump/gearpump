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

import java.net.InetAddress
import akka.actor._
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.gearpump.experiments.yarn.{ContainerLaunchContextFactory, NodeManagerCallbackHandlerFactory, TestConfiguration}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfter, FlatSpecLike}
import org.slf4j.Logger
import org.specs2.mock.Mockito
import scala.util.{Failure, Success}

class MockedChild(probe: ActorRef) extends Actor {
  val LOG: Logger = LogUtil.getLogger(getClass)

  override def receive: Receive = {
    case x =>  probe ! x
  }
}

class AmActorSpec extends TestKit(ActorSystem("testsystem"))
with FlatSpecLike
with StopSystemAfterAll
with BeforeAndAfter
with Mockito
with TestConfiguration {
  import AmActorProtocol._

  //warning : one for all tests
  val containerLaunchContextMock = mock[ContainerLaunchContext]
  var probe: TestProbe = _
  var amActor: TestActorRef[AmActor] = _
  var nmClientAsyncMock: NMClientAsync = _

  private[this] def getRegisterAMMessage: RegisterAMMessage = {
    val port = appConfig.getEnv(YARNAPPMASTER_PORT).toInt
    val host = InetAddress.getLocalHost.getHostName
    val target = host + ":" + port
    val addr = NetUtils.createSocketAddr(target)
    val trackingURL = "http://" + host + ":" + appConfig.getEnv(SERVICES_PORT).toInt
    new RegisterAMMessage(addr.getHostName, port, trackingURL)
  }

  private[this] def createAmActor(probe: TestProbe): TestActorRef[AmActor] = {

    val amActorProps = Props(new AmActor(appConfig,
      yarnConfiguration,
      AmActor.RMCallbackHandlerActorProps(Props(classOf[MockedChild], probe.ref)),
      AmActor.RMClientActorProps(Props(classOf[MockedChild], probe.ref)),
      getNMClientAsyncFactoryMock,
      mock[NodeManagerCallbackHandlerFactory],
      getContainerContextFactoryMock
    ))
    TestActorRef[AmActor](amActorProps)
  }

  private def getContainerContextFactoryMock: ContainerLaunchContextFactory = {
    mock[ContainerLaunchContextFactory].newInstance(anyString) returns containerLaunchContextMock
  }

  private def getNMClientAsyncFactoryMock: NMClientAsyncFactory = {
    mock[NMClientAsyncFactory].newInstance(any, any) returns nmClientAsyncMock
  }

  private def getContainerId():ContainerId = {
    ContainerId.newInstance(ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1), 1)
  }

  private def getContainer: Container = {
    getContainer(NodeId.newInstance(TEST_MASTER_HOSTNAME, 50000))
  }
  private def getContainer(nodeId: NodeId):Container = {
    val token = Token.newInstance(Array[Byte](), "kind",  Array[Byte](), "service")
    Container.newInstance(getContainerId(), nodeId, "", Resource.newInstance(1024, 2), Priority.newInstance(1), token)
  }

  private def expectTermination(probe: TestProbe, target: ActorRef): Terminated = {
    probe watch target
    probe.expectTerminated(target)
  }

  before {
    nmClientAsyncMock = mock[NMClientAsync]
    probe = TestProbe()
    amActor = createAmActor(probe)

  }

  "An AmActor" should "resend ResourceManagerCallbackHandler message to ResourceManagerClientActor" in {
    val msg = new ResourceManagerCallbackHandler(appConfig, probe.ref)
    amActor ! msg
    probe.expectMsg(msg)
    probe.expectNoMsg
  }

  "An AmActor" should "send RegisterAMMessage to ResourceManagerClientActor when receiving AMRMCientAsyncStartup(Success(true))" in {
    val msg = AMRMClientAsyncStartup(status=Success(true))
    amActor ! msg
    val port = appConfig.getEnv(YARNAPPMASTER_PORT).toInt
    val host = amActor.underlyingActor.host
    val target = host + ":" + port
    val addr = NetUtils.createSocketAddr(target)
    val trackingURL = amActor.underlyingActor.trackingURL
    probe.expectMsg(new RegisterAMMessage(addr.getHostName, port, trackingURL))
    probe.expectNoMsg
  }

  "An AmActor" should "not send any messages to ResourceManagerClientActor when receiving AMRMCientAsyncStartup(Success(false)) or AMRMCientAsyncStartup(Failure)" in {
    amActor ! AMRMClientAsyncStartup(status=Success(false))
    probe.expectNoMsg
    amActor ! AMRMClientAsyncStartup(status=Failure(new RuntimeException()))
    probe.expectNoMsg

  }

  "An AmActor" should "send one or more ContainerRequestMessages to ResourceManagerClientActor when receiving a RegisterAppMasterResponse" in {
    val response = mock[RegisterApplicationMasterResponse]
    response.getContainersFromPreviousAttempts returns java.util.Collections.emptyList[Container]()
    val msg = RegisterAppMasterResponse(response)
    amActor ! msg
    probe.expectMsgAllOf(ContainerRequestMessage(appConfig.getEnv(GEARPUMPMASTER_MEMORY).toInt, appConfig.getEnv(GEARPUMPMASTER_VCORES).toInt),
      ContainerRequestMessage(appConfig.getEnv(GEARPUMPMASTER_MEMORY).toInt, appConfig.getEnv(GEARPUMPMASTER_VCORES).toInt))
    probe.expectNoMsg
  }

  "An AmActor" should "stop nodeManagerClient, send AMStatusMessage to RMClientActor with Succeed status and stop it when receiving RMHandlerDone with AllRequestedContainersCompleted reason" in {
    val stats = RMHandlerContainerStats(3, 3, 0)
    amActor ! RMHandlerDone(AmStates.AllRequestedContainersCompleted, stats)
    one(nmClientAsyncMock).stop()
    val message = s"Diagnostics. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
    probe.expectMsg(AMStatusMessage(FinalApplicationStatus.SUCCEEDED, message, null))
    noMoreCallsTo(nmClientAsyncMock)
    expectTermination(probe, amActor.underlyingActor.rmClientActor)
    probe.expectNoMsg()
  }

  "An AmActor" should "stop nodeManagerClient, send AMStatusMessage to RMClientActor with Failed status and stop it when receiving RMHandlerDone with Failed reason" in {
    val stats = RMHandlerContainerStats(3, 0, 3)
    amActor ! RMHandlerDone(AmStates.Failed(new RuntimeException), stats)
    one(nmClientAsyncMock).stop()
    val message = s"Failed. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
    probe.expectMsg(AMStatusMessage(FinalApplicationStatus.FAILED, message, null))
    noMoreCallsTo(nmClientAsyncMock)
    expectTermination(probe, amActor.underlyingActor.rmClientActor)
    probe.expectNoMsg()
  }

  "An AmActor" should "stop nodeManagerClient, send AMStatusMessage to RMClientActor with Killed status end stop it when receiving RMHandlerDone with ShutdownRequest reason" in {
    val stats = RMHandlerContainerStats(3, 3, 0)
    amActor ! RMHandlerDone(AmStates.ShutdownRequest, stats)
    one(nmClientAsyncMock).stop()
    val message = s"ShutdownRequest. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
    probe.expectMsg(AMStatusMessage(FinalApplicationStatus.KILLED, message, null))
    noMoreCallsTo(nmClientAsyncMock)
    expectTermination(probe, amActor.underlyingActor.rmClientActor)
    probe.expectNoMsg()
  }

  "An AmActor" should "stop nodeManagerClient, send AMStatusMessage to RMClientActor with Failed status end stop it when receiving RMHandlerDone with ShutdownRequest reason and any containers failed" in {
    val stats = RMHandlerContainerStats(3, 0, 3)
    amActor ! RMHandlerDone(AmStates.ShutdownRequest, stats)
    one(nmClientAsyncMock).stop()
    val message = s"ShutdownRequest. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
    probe.expectMsg(AMStatusMessage(FinalApplicationStatus.FAILED, message, null))
    noMoreCallsTo(nmClientAsyncMock)
    expectTermination(probe, amActor.underlyingActor.rmClientActor)
    probe.expectNoMsg()
  }

  "An AmActor" should  "resend the same message to RMClientActor when ContainerRequestMessage is send to it" in {
    amActor ! ContainerRequestMessage(1024, 1)
    probe.expectMsg(ContainerRequestMessage(1024, 1))
  }

  "An AmActor with 0 masters started" should "set masterAddr and start container(s) for master(s) using nodeManagerClient when receiving LanuchContainers" in {
    amActor.underlyingActor.masterContainersStarted = 0
    amActor.underlyingActor.masterAddr = None
    val container1 = getContainer(NodeId.newInstance("host1", 3001))
    val container2 = getContainer(NodeId.newInstance("host2", 3002))
    amActor ! LaunchContainers(List(container1, container2))
    amActor.underlyingActor.masterAddr.get shouldBe HostPort("host1:3001")
    one(nmClientAsyncMock).startContainerAsync(container1, containerLaunchContextMock)
    one(nmClientAsyncMock).startContainerAsync(container2, containerLaunchContextMock)
    noMoreCallsTo(nmClientAsyncMock)
    // TODO this should show assertion error but it doesn't work (probably some traits related problem)
    // there was one(nmClientAsyncMock).startContainerAsync(container2, containerLaunchContextMock)
  }

  "An AmActor with enough masters started" should "inc workerContainerRequested and start container(s) for workers(s) using nodeManagerClient when receiving LanuchContainers" in {
    amActor.underlyingActor.masterContainersStarted = TEST_MASTER_CONTAINERS
    amActor.underlyingActor.workerContainersStarted = 0
    amActor.underlyingActor.workerContainersRequested = 0
    amActor.underlyingActor.masterAddr = Some(HostPort(s"$TEST_MASTER_HOSTNAME:$TEST_MASTER_PORT"))
    amActor ! LaunchContainers(List(getContainer, getContainer, getContainer))
    amActor.underlyingActor.workerContainersRequested shouldBe 3
    3.times(nmClientAsyncMock).startContainerAsync(getContainer, containerLaunchContextMock)
    noMoreCallsTo(nmClientAsyncMock)
  }


  "An AmActor without enough masters started" should "only increment started masters counter when ContainerStarted is send to it" in {
    amActor.underlyingActor.masterContainersStarted = 0
    amActor ! ContainerStarted(getContainerId())
    amActor.underlyingActor.masterContainersStarted shouldBe 1
    probe.expectNoMsg
  }

  "An AmActor that just started enough masters" should "increment started masters counter and request all worker containers when ContainerStarted is send to it" in {
    amActor.underlyingActor.masterContainersStarted = TEST_MASTER_CONTAINERS - 1
    amActor.underlyingActor.workerContainersStarted = 0
    amActor ! ContainerStarted(getContainerId())
    amActor.underlyingActor.masterContainersStarted shouldBe TEST_MASTER_CONTAINERS
    //TODO: Number of ContainerRequestMessage should depends on WORKER_CONTAINER_COUNT
    probe.expectMsgAllOf(ContainerRequestMessage(appConfig.getEnv(WORKER_MEMORY).toInt, appConfig.getEnv(WORKER_VCORES).toInt),
      ContainerRequestMessage(appConfig.getEnv(WORKER_MEMORY).toInt, appConfig.getEnv(WORKER_VCORES).toInt),
      ContainerRequestMessage(appConfig.getEnv(WORKER_MEMORY).toInt, appConfig.getEnv(WORKER_VCORES).toInt))
    probe.expectNoMsg
  }

  "An AmActor that started enough masters but not enough workers" should "only increment started worker counter when ContainerStarted is send to it" in {
    amActor.underlyingActor.masterContainersStarted = TEST_MASTER_CONTAINERS
    amActor.underlyingActor.workerContainersStarted = TEST_WORKER_CONTAINERS - 2
    amActor ! ContainerStarted(getContainerId())
    amActor.underlyingActor.masterContainersStarted shouldBe TEST_MASTER_CONTAINERS
    probe.expectNoMsg()
  }

  "An AmActor that started enough workers" should "start services actor" in {
    //how to test that?
  }

}