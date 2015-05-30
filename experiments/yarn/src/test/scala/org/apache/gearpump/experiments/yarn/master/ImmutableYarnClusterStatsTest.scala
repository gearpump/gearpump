package org.apache.gearpump.experiments.yarn.master

import org.apache.gearpump.experiments.yarn.ContainerHelper._
import org.apache.gearpump.experiments.yarn.master.AmActorProtocol._
import org.apache.hadoop.yarn.api.records._
import org.scalatest.FlatSpecLike
import org.specs2.mock.Mockito
import org.scalatest.Matchers._

class ImmutableYarnClusterStatsTest extends FlatSpecLike with Mockito {

  "The ImmutableYarnClusterStats" should
    "return ImmutableYarnClusterStats with new Container when withContainer method is called" in {
    val containerId = 1
    val containerInfo = ContainerInfo("host", 3000, MASTER, getContainerStatus(containerId, ContainerState.NEW))
    val stats = YarnClusterStats(1, 1).withContainer(containerInfo)
    containerInfo should be theSameInstanceAs(stats.getContainerInfo(getContainerId(containerId)).get)
  }

  it should
    "return correct number of running containers when getRunningContainersCount method is called" in {
    val containerInfo = ContainerInfo("host", 3000, MASTER, getContainerStatus(1, ContainerState.NEW))
    val runningContainer1 = ContainerInfo("host", 3000, MASTER, getContainerStatus(2, ContainerState.RUNNING))
    val runningContainer2 = ContainerInfo("host", 3000, MASTER, getContainerStatus(3, ContainerState.RUNNING))
    val stats = YarnClusterStats(1, 1).withContainer(runningContainer1).withContainer(runningContainer2).withContainer(containerInfo)
    stats.getRunningContainersCount(MASTER) should be(2)
  }

  it should "return Some(ImmutableYarnClusterStats) with updated Container when withUpdatedContainer(existingContainerStatus) method is called" in {
    val containerId = 1
    val containerInfo = ContainerInfo("host", 3000, MASTER, getContainerStatus(containerId, ContainerState.NEW))
    val status = getContainerStatus(containerId, ContainerState.RUNNING)
    val maybeClusterStats = YarnClusterStats(1, 1).withContainer(containerInfo).withUpdatedContainer(status)
    maybeClusterStats.isDefined should be(true)
    val updatedContainerInfo = maybeClusterStats.get.getContainerInfo(getContainerId(containerId)).get
    updatedContainerInfo.status.getState should be(ContainerState.RUNNING)
  }

  it should "return Option.None when withUpdatedContainer(nonExistingContainerStatus) method is called" in {
    val containerInfo = ContainerInfo("host", 3000, MASTER, getContainerStatus(1, ContainerState.NEW))
    val maybeClusterStats = YarnClusterStats(1, 1).withContainer(containerInfo).withUpdatedContainer(getContainerStatus(2, ContainerState.RUNNING))
    maybeClusterStats.isEmpty should be(true)
  }

  it should "return correct completed containers count when getCompletedContainersCount method is called" in {
    val containerInfo = ContainerInfo("host", 3000, MASTER, getContainerStatus(1, ContainerState.RUNNING))
    val runningContainer1 = ContainerInfo("host", 3000, MASTER, getContainerStatus(2, ContainerState.COMPLETE))
    val runningContainer2 = ContainerInfo("host", 3000, MASTER, getContainerStatus(3, ContainerState.COMPLETE))
    val stats = YarnClusterStats(1, 1).withContainer(runningContainer1).withContainer(runningContainer2).withContainer(containerInfo)
    stats.getCompletedContainersCount(MASTER) should be(2)
  }

  it should "return running masters addresses when getRunningMasterContainersAddrs method is called" in {
    val containerInfo = ContainerInfo("host", 3000, MASTER, getContainerStatus(1, ContainerState.COMPLETE))
    val runningContainer1 = ContainerInfo("host1", 3001, MASTER, getContainerStatus(2, ContainerState.RUNNING))
    val runningContainer2 = ContainerInfo("host2", 3002, MASTER, getContainerStatus(3, ContainerState.RUNNING))
    val stats = YarnClusterStats(1, 1).withContainer(runningContainer1).withContainer(runningContainer2).withContainer(containerInfo)
    val masters = stats.getRunningMasterContainersAddrs
    masters should contain theSameElementsAs Vector("host1:3001", "host2:3002")

  }

  it should "return proper ContainerStats when containerStats method is called" in  {
    val completedContainer1 = ContainerInfo("host", 3000, WORKER, getContainerStatus(1, ContainerState.COMPLETE))
    //completed with exitCode 3, should be counted as failed
    val completedContainer2 = ContainerInfo("host", 3003, WORKER, ContainerStatus.newInstance(getContainerId(4), ContainerState.COMPLETE, "", 3))
    val runningContainer1 = ContainerInfo("host1", 3001, MASTER, getContainerStatus(2, ContainerState.RUNNING))
    val runningContainer2 = ContainerInfo("host2", 3002, MASTER, getContainerStatus(3, ContainerState.RUNNING))

    val stats = YarnClusterStats(1, 1).
      withContainer(runningContainer1).
      withContainer(runningContainer2).
      withContainer(completedContainer1).
      withContainer(completedContainer2)
    stats.containerStats should be(ContainerStats(4, 1, 3, 2))

  }

}
