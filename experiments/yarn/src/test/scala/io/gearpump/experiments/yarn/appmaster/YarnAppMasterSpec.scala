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

package io.gearpump.experiments.yarn.appmaster

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{TestActorRef, TestProbe}
import com.typesafe.config.ConfigFactory
import io.gearpump.cluster.TestUtil
import io.gearpump.experiments.yarn.Constants
import io.gearpump.experiments.yarn.appmaster.YarnAppMaster.{ActiveConfig, AddWorker, AppMasterRegistered, ClusterInfo, CommandResult, ContainerStarted, ContainersAllocated, GetActiveConfig, Kill, QueryClusterInfo, QueryVersion, RemoveWorker, ResourceManagerException, Version}
import io.gearpump.experiments.yarn.appmaster.YarnAppMasterSpec.UI
import io.gearpump.experiments.yarn.glue.Records.{Container, Resource, _}
import io.gearpump.experiments.yarn.glue.{NMClient, RMClient}
import io.gearpump.transport.HostPort
import org.mockito.ArgumentCaptor
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class YarnAppMasterSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  val config = ConfigFactory.parseString(

    """
      |gearpump {
      |  yarn {
      |    client {
      |      package -path = "/user/gearpump/gearpump.zip"
      |    }
      |
      |    applicationmaster {
      |      ## Memory of YarnAppMaster
      |        command = "$JAVA_HOME/bin/java -Xmx512m"
      |      memory = "512"
      |      vcores = "1"
      |      queue = "default"
      |    }
      |
      |    master {
      |      ## Memory of master daemon
      |      command = "$JAVA_HOME/bin/java  -Xmx512m"
      |      memory = "512"
      |      vcores = "1"
      |    }
      |
      |    worker {
      |      ## memory of worker daemon
      |      command = "$JAVA_HOME/bin/java  -Xmx512m"
      |      containers = "4"
      |      ## This also contains all memory for child executors.
      |      memory = "4096"
      |      vcores = "1"
      |    }
      |    services {
      |      enabled = true
      |    }
      |  }
      |}
    """.stripMargin).withFallback(TestUtil.DEFAULT_CONFIG)

  val masterCount = 1
  val workerCount = config.getString(Constants.WORKER_CONTAINERS).toInt

  implicit var system: ActorSystem = null
  val packagePath = "/user/gearpump/gearpump.zip"
  val configPath = "/user/my/conf"


  override def beforeAll() = {
    system = ActorSystem("test", config)
  }

  override def afterAll() = {
    system.shutdown()
    system.awaitTermination()
  }

  it should "start master, worker and UI on YARN" in {
    val rmClient = mock(classOf[RMClient])
    val nmClient = mock(classOf[NMClient])
    val ui = mock(classOf[UIFactory])
    when(ui.props(any[List[HostPort]], anyString, anyInt)).thenReturn(Props(new UI))


    val appMaster = TestActorRef(Props(new YarnAppMaster(config, rmClient, nmClient, packagePath, configPath, ui)))

    verify(rmClient).start(appMaster)
    verify(nmClient).start(appMaster)
    verify(rmClient).registerAppMaster(anyString, anyInt, anyString)

    appMaster ! AppMasterRegistered

    val masterResources = ArgumentCaptor.forClass(classOf[List[Resource]])
    verify(rmClient).requestContainers(masterResources.capture())
    assert(masterResources.getValue.size == masterCount)

    val masterContainer = mock(classOf[Container])
    val mockNode = mock(classOf[NodeId])
    val mockId = mock(classOf[ContainerId])
    when(masterContainer.getNodeId).thenReturn(mockNode)
    when(masterContainer.getId).thenReturn(mockId)

    // launch master
    appMaster ! ContainersAllocated(List.fill(masterCount)(masterContainer))
    verify(nmClient, times(masterCount)).launchCommand(any[Container], anyString, anyString, anyString)

    // master containers started
    (0 until masterCount).foreach(_ => appMaster ! ContainerStarted(mockId))

    //transition to start workers
    val workerResources = ArgumentCaptor.forClass(classOf[List[Resource]])
    verify(rmClient, times(2)).requestContainers(workerResources.capture())
    assert(workerResources.getValue.size == workerCount)

    // launch workers
    val workerContainer = mock(classOf[Container])
    when(workerContainer.getNodeId).thenReturn(mockNode)
    val workerContainerId = ContainerId.fromString("container_1449802454214_0034_01_000006")
    when(workerContainer.getId).thenReturn(workerContainerId)
    appMaster ! ContainersAllocated(List.fill(workerCount)(workerContainer))
    verify(nmClient, times(workerCount + masterCount)).launchCommand(any[Container], anyString, anyString, anyString)

    // worker containers started
    (0 until workerCount).foreach(_ => appMaster ! ContainerStarted(mockId))

    // start UI server
    verify(ui, times(1)).props(any[List[HostPort]], anyString, anyInt)

    //Application Ready...
    val client = TestProbe()

    // get active config
    appMaster.tell(GetActiveConfig("client"), client.ref)
    client.expectMsgType[ActiveConfig]

    // query version
    appMaster.tell(QueryVersion, client.ref)
    client.expectMsgType[Version]

    // query version
    appMaster.tell(QueryClusterInfo, client.ref)
    client.expectMsgType[ClusterInfo]

    // add worker
    val newWorkerCount = 2
    appMaster.tell(AddWorker(newWorkerCount), client.ref)
    client.expectMsgType[CommandResult]
    val newWorkerResources = ArgumentCaptor.forClass(classOf[List[Resource]])
    verify(rmClient, times(3)).requestContainers(newWorkerResources.capture())
    assert(newWorkerResources.getValue.size == newWorkerCount)

    // new container allocated
    appMaster ! ContainersAllocated(List.fill(newWorkerCount)(workerContainer))
    verify(nmClient, times(workerCount + masterCount + newWorkerCount)).
      launchCommand(any[Container], anyString, anyString, anyString)

    // new worker containers started
    (0 until newWorkerCount).foreach(_ => appMaster ! ContainerStarted(mockId))

    // same UI server
    verify(ui, times(1)).props(any[List[HostPort]], anyString, anyInt)

    // remove worker
    appMaster.tell(RemoveWorker(workerContainerId.toString), client.ref)
    client.expectMsgType[CommandResult]
    verify(nmClient).stopContainer(any[ContainerId], any[NodeId])

    // kill the app
    appMaster.tell(Kill, client.ref)
    client.expectMsgType[CommandResult]
    verify(nmClient, times(1)).stop()
    verify(rmClient, times(1)).shutdownApplication()

    // on error
    val ex = new Exception
    appMaster.tell(ResourceManagerException(ex), client.ref)
    verify(nmClient, times(2)).stop()
    verify(rmClient, times(1)).failApplication(ex)
  }
}

object YarnAppMasterSpec {

  class UI extends Actor {
    def receive: Receive = null
  }
}