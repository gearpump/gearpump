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

package io.gearpump.experiments.yarn.master

import akka.actor._
import akka.testkit._
import io.gearpump.experiments.yarn.{ContainerHelper, TestConfiguration, Constants}
import Constants._
import io.gearpump.util.LogUtil
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.scalatest.FreeSpecLike
import org.scalatest.Matchers._
import org.slf4j.Logger
import ContainerHelper._

class MockedChild(probe: ActorRef) extends Actor {
  val LOG: Logger = LogUtil.getLogger(getClass)

  override def receive: Receive = {
    case x =>  probe ! x
  }
}

class YarnApplicationMasterSpec extends TestKit(ActorSystem("testsystem"))
with FreeSpecLike
with StopSystemAfterAll
with TestConfiguration {

  import AmActorProtocol._

  //warning : one for all tests
  val containerLaunchContextMock = mock[ContainerLaunchContext]

  private[this] def createAmActor(probe: TestProbe, nmClientAsyncMock: NMClientAsync): TestActorRef[YarnApplicationMaster] = {

    val amActorProps = Props(new YarnApplicationMaster(appConfig,
      yarnConfiguration,
      Props(classOf[MockedChild], probe.ref),
      (config, cb) => nmClientAsyncMock,
      (command) => containerLaunchContextMock,
      mock[YarnClusterStats]
    ))
    TestActorRef[YarnApplicationMaster](amActorProps)
  }

  private[this] def createFSM(probe: TestProbe,
                              nmClientAsyncMock: NMClientAsync,
                              yarnClusterStats: YarnClusterStats): TestFSMRef[State, YarnClusterStats, YarnApplicationMaster] = {
    TestFSMRef(new YarnApplicationMaster(appConfig,
      yarnConfiguration,
      Props(classOf[MockedChild], probe.ref),
      (config, cb) => nmClientAsyncMock,
      (command) => containerLaunchContextMock,
      yarnClusterStats
    ))
  }

  private def expectTermination(probe: TestProbe, target: ActorRef): Terminated = {
    probe watch target
    probe.expectTerminated(target)
  }

  "The YarnApplicationMaster" - {
    "should start in ConnectingToResourceManager state" in {
      val probe = TestProbe()
      val nmClientAsyncMock = mock[NMClientAsync]
      val fsm = createFSM(probe, nmClientAsyncMock, mock[YarnClusterStats])
      fsm.stateName should be(ConnectingToResourceManager)
    }
  }

  "The YarnApplicationMaster" - {
    "in ConnectingToResourceManager state" - {
      "when receiving RMConnected" - {
        "should send RegisterAMMessage to ResourceManagerClient and move to RegisteringAppMaster state" in {
          val probe = TestProbe()
          val fsm = createFSM(probe, mock[NMClientAsync], mock[YarnClusterStats])
          fsm.setState(ConnectingToResourceManager)
          fsm ! RMConnected
          val port = appConfig.getEnv(YARNAPPMASTER_PORT).toInt
          val host = fsm.underlyingActor.host
          val target = host + ":" + port
          val addr = NetUtils.createSocketAddr(target)
          val trackingURL = fsm.underlyingActor.trackingURL
          probe.expectMsg(new RegisterAMMessage(addr.getHostName, port, trackingURL))
          fsm.stateName should be(RegisteringAppMaster)
        }
      }
    }
  }

  "when receiving RMConnectionFailed" - {
    "should stop nodeManagerClient, send PoisonPill to resourceManagerClient and stay in ConnectingToResourceManager state" in {
      val probe = TestProbe()
      val nmClientAsyncMock = mock[NMClientAsync]
      val fsm = createFSM(probe, nmClientAsyncMock, mock[YarnClusterStats])
      fsm.setState(ConnectingToResourceManager)
      val mockedThrowable = mock[Throwable]
      fsm ! RMConnectionFailed(mockedThrowable)
      one(nmClientAsyncMock).stop
      expectTermination(probe, fsm.underlyingActor.resourceManagerClient)
      fsm.stateName should be(ConnectingToResourceManager)
    }
  }

  /**
   * TODO: previous attempt thing...
   */
  "The YarnApplicationMaster" - {
    "in RegisteringAppMaster state" - {
      "when receiving RegisterAppMasterResponse" - {
        "should send correct ContainersRequest to ResourceManagerClient and move to LaunchingMasters state" in {
          val probe = TestProbe()
          val fsm = createFSM(probe, mock[NMClientAsync], mock[YarnClusterStats])
          fsm.setState(RegisteringAppMaster)

          val response = mock[RegisterApplicationMasterResponse]
          response.getContainersFromPreviousAttempts returns java.util.Collections.emptyList[Container]()
          fsm ! RegisterAppMasterResponse(response)
          val containers = List[Resource](
            Resource.newInstance(appConfig.getEnv(GEARPUMPMASTER_MEMORY).toInt, appConfig.getEnv(GEARPUMPMASTER_VCORES).toInt),
            Resource.newInstance(appConfig.getEnv(GEARPUMPMASTER_MEMORY).toInt, appConfig.getEnv(GEARPUMPMASTER_VCORES).toInt)
          )
          probe.expectMsg(ContainersRequest(containers))
          fsm.stateName should be(LaunchingMasters)
        }
      }
    }
  }


  "The YarnApplicationMaster" - {
    "in LaunchingMasters state" - {
      "when receiving ContainersAllocated" - {
        "should start containers for masters using nodeManagerClient and stay in LaunchingMasters state with updated YarnClusterStats" in {
          val probe = TestProbe()
          val nmClientAsyncMock = mock[NMClientAsync]
          val container = getContainer(NodeId.newInstance("host1", 3002))
          val stats = mock[YarnClusterStats]
          val fsm = createFSM(probe, nmClientAsyncMock, stats)
          val expectedContainerInfo = fsm.underlyingActor.newContainerInfoInstance(container, MASTER, TEST_MASTER_PORT.toInt)

          stats.withContainer(any) returns stats
          stats.getContainerInfo(container.getId) returns None
          fsm.setState(LaunchingMasters, stats)

          fsm ! ContainersAllocated(List(container))
          one(nmClientAsyncMock).startContainerAsync(container, containerLaunchContextMock)
          one(stats).withContainer(expectedContainerInfo)
          no(stats).withUpdatedContainer(any)
          fsm.stateName should be(LaunchingMasters)
        }
      }
    }
  }


  "when receiving ContainerStarted" - {
    "if without enough masters started should stay in LaunchingMasters state with updated stats" in {
      val probe = TestProbe()
      val nmClientAsyncMock = mock[NMClientAsync]
      val stats = mock[YarnClusterStats]
      val fsm = createFSM(probe, nmClientAsyncMock, stats)
      stats.withUpdatedContainer(any) returns Some(stats)
      stats.getRunningContainersCount(MASTER) returns TEST_MASTER_CONTAINERS - 1
      fsm.setState(LaunchingMasters, stats)

      val containerId = getContainerId
      fsm ! ContainerStarted(containerId)
      fsm.stateName should be(LaunchingMasters)
      one(stats).withUpdatedContainer(ContainerStatus.newInstance(containerId, ContainerState.RUNNING, "", 0))
      noCallsTo(nmClientAsyncMock)
    }
  }

  "if enough masters started should move to the LaunchingWorkers state with updated stats" in {
    val probe = TestProbe()
    val nmClientAsyncMock = mock[NMClientAsync]
    val stats = mock[YarnClusterStats]
    val fsm = createFSM(probe, nmClientAsyncMock, stats)
    stats.withUpdatedContainer(any) returns Some(stats)
    stats.getRunningContainersCount(MASTER) returns TEST_MASTER_CONTAINERS
    fsm.setState(LaunchingMasters, stats)

    val containerId = getContainerId
    fsm ! ContainerStarted(containerId)
    fsm.stateName should be(LaunchingWorkers)
    one(stats).withUpdatedContainer(ContainerStatus.newInstance(containerId, ContainerState.RUNNING, "", 0))
    noCallsTo(nmClientAsyncMock)
  }

  "The YarnApplicationMaster" - {
    "in LaunchingWorkers state" - {
      "when receiving ContainersAllocated" - {
        "should start containers for workers using nodeManagerClient and stay in LaunchingWorkers state with updated YarnClusterStats" in {
          val probe = TestProbe()
          val nmClientAsyncMock = mock[NMClientAsync]
          val container = getContainer(NodeId.newInstance("host1", 3001))
          val stats = mock[YarnClusterStats]
          val fsm = createFSM(probe, nmClientAsyncMock, stats)
          val expectedContainerInfo = fsm.underlyingActor.newContainerInfoInstance(container, WORKER, 3001)

          stats.withContainer(any) returns stats
          stats.getContainerInfo(container.getId) returns None
          stats.getRunningMasterContainersAddrs returns Array("master:3000")

          fsm.setState(LaunchingWorkers, stats)

          fsm ! ContainersAllocated(List(container))
          one(nmClientAsyncMock).startContainerAsync(container, containerLaunchContextMock)
          one(stats).withContainer(expectedContainerInfo)
          no(stats).withUpdatedContainer(any)
          fsm.stateName should be(LaunchingWorkers)
        }
      }
    }
  }

  "if there is no containerId that was allocated should stay in LaunchingWorkers state without updated YarnClusterStats" in {
    val probe = TestProbe()
    val nmClientAsyncMock = mock[NMClientAsync]
    val container = getContainer(NodeId.newInstance("host1", 3001))
    val stats = mock[YarnClusterStats]
    val fsm = createFSM(probe, nmClientAsyncMock, stats)
    val expectedContainerInfo = fsm.underlyingActor.newContainerInfoInstance(container, WORKER, 3001)

    stats.withContainer(any) returns stats
    stats.getContainerInfo(container.getId) returns Some(expectedContainerInfo)
    stats.getRunningMasterContainersAddrs returns Array("master:3000")

    fsm.setState(LaunchingWorkers, stats)

    fsm ! ContainersAllocated(List(container))
    no(nmClientAsyncMock).startContainerAsync(any, any)
    no(stats).withContainer(any)
    no(stats).withUpdatedContainer(any)
    fsm.stateName should be(LaunchingWorkers)
    fsm.stateData should be theSameInstanceAs stats
  }

  "when receiving ContainerStarted" - {
    "if without enough workers started should stay in LaunchingWorkers state with updated stats" in {
      val probe = TestProbe()
      val nmClientAsyncMock = mock[NMClientAsync]
      val stats = mock[YarnClusterStats]
      val fsm = createFSM(probe, nmClientAsyncMock, stats)
      stats.withUpdatedContainer(any) returns Some(stats)
      stats.getRunningContainersCount(WORKER) returns TEST_WORKER_CONTAINERS - 2
      fsm.setState(LaunchingWorkers, stats)

      val containerId = getContainerId
      fsm ! ContainerStarted(containerId)
      fsm.stateName should be(LaunchingWorkers)
      one(stats).withUpdatedContainer(ContainerStatus.newInstance(containerId, ContainerState.RUNNING, "", 0))
      noCallsTo(nmClientAsyncMock)
    }
  }


  "if there is no relevant ContainerId in YarnClusterStats it should stay in LaunchingWorkers state without updating stats" in {
    val stats = mock[YarnClusterStats]
    val fsm = createFSM(TestProbe(), mock[NMClientAsync], stats)
    stats.withUpdatedContainer(any) returns None
    stats.getRunningContainersCount(WORKER) returns TEST_WORKER_CONTAINERS - 2
    fsm.setState(LaunchingWorkers, stats)
    val containerId = getContainerId
    fsm ! ContainerStarted(containerId)
    fsm.stateName should be(LaunchingWorkers)
    fsm.stateData should be theSameInstanceAs stats
  }

  "if enough workers started should start Services actor and move to the AwaitingTermination state and with updated stats" in {
    val probe = TestProbe()
    val nmClientAsyncMock = mock[NMClientAsync]
    val stats = mock[YarnClusterStats]
    val fsm = createFSM(probe, nmClientAsyncMock, stats)
    stats.withUpdatedContainer(any) returns Some(stats)
    stats.getRunningContainersCount(WORKER) returns TEST_WORKER_CONTAINERS
    fsm.setState(LaunchingWorkers, stats)

    val containerId = getContainerId
    fsm ! ContainerStarted(containerId)
    fsm.stateName should be(AwaitingTermination)
    one(stats).withUpdatedContainer(ContainerStatus.newInstance(containerId, ContainerState.RUNNING, "", 0))
    fsm.underlyingActor.servicesEnabled match {
      case true =>
        fsm.underlyingActor.servicesActor.isDefined should be(true)
      case false =>
    }
    noCallsTo(nmClientAsyncMock)
  }

  "The YarnApplicationMaster" - {
    "in AwaitingTermination state" - {
      "when receiving ContainersCompleted" - {
        "if not all containers completed should stay in AwaitingTermination state with updated YarnClusterStats" in {
          val probe = TestProbe()
          val nmClientAsyncMock = mock[NMClientAsync]
          val stats = mock[YarnClusterStats]
          val fsm = createFSM(probe, nmClientAsyncMock, stats)

          fsm.setState(AwaitingTermination, stats)
          stats.withUpdatedContainer(any) returns Some(stats)
          stats.getCompletedContainersCount(MASTER) returns TEST_MASTER_CONTAINERS - 1
          stats.getCompletedContainersCount(WORKER) returns 0

          val containerStatus = ContainerStatus.newInstance(getContainerId, ContainerState.COMPLETE, "", 0)
          val containersCompleted = ContainersCompleted(List(containerStatus, containerStatus))

          fsm ! containersCompleted
          fsm.stateName should be(AwaitingTermination)
          two(stats).withUpdatedContainer(containerStatus)
        }
      }
    }
  }

  "if all containers completed should stop nodeManagerClient, send AMShutdownRequest to ResourceManagerClient and stay in AwaitingTermination state with updated YarnClusterStats" in {
    val probe = TestProbe()
    val nmClientAsyncMock = mock[NMClientAsync]
    val stats = mock[YarnClusterStats]
    val fsm = createFSM(probe, nmClientAsyncMock, stats)

    fsm.setState(AwaitingTermination, stats)
    stats.withUpdatedContainer(any) returns Some(stats)
    stats.getCompletedContainersCount(MASTER) returns TEST_MASTER_CONTAINERS
    stats.getCompletedContainersCount(WORKER) returns TEST_WORKER_CONTAINERS
    val containerStats = ContainerStats(1, 1, 0, 1)
    stats.containerStats returns containerStats
    val containerStatus = ContainerStatus.newInstance(getContainerId, ContainerState.COMPLETE, "", 0)
    val containersCompleted = ContainersCompleted(List(containerStatus))

    fsm ! containersCompleted
    fsm.stateName should be(AwaitingTermination)
    one(nmClientAsyncMock).stop
    probe.expectMsg(AMShutdownRequest(containerStats))
  }

  "The YarnApplicationMaster" - {
    "in any state" - {
      "when receiving RMTerminalState" - {
        "should stop nodeManagerClient, send PoisonPill to ResourceManagerClient and stay in current state" in {
          val probe = TestProbe()
          val nmClientAsyncMock = mock[NMClientAsync]
          val stats = mock[YarnClusterStats]
          val fsm = createFSM(probe, nmClientAsyncMock, stats)

          fsm.setState(AwaitingTermination, stats)
          val containerStats = ContainerStats(1, 1, 0, 1)
          stats.containerStats returns containerStats

          fsm ! RMShutdownRequest
          fsm.stateName should be(AwaitingTermination)
          one(nmClientAsyncMock).stop
          expectTermination(probe, fsm.underlyingActor.resourceManagerClient)
        }
      }
    }
  }

  "The YarnApplicationMaster" - {
    "in any state" - {
      "when receiving unknown message" - {
        "should stay in current state" in {
          val fsm = createFSM(TestProbe(), mock[NMClientAsync], mock[YarnClusterStats])

          fsm.setState(AwaitingTermination)
          fsm ! Some
          fsm.stateName should be(AwaitingTermination)
        }
      }
    }
  }
}