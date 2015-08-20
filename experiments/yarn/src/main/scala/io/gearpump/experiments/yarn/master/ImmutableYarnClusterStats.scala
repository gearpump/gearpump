package io.gearpump.experiments.yarn.master

import AmActorProtocol._
import io.gearpump.util.LogUtil
import org.apache.hadoop.yarn.api.records.{ContainerState, ContainerStatus, ContainerId}
import org.slf4j.Logger

trait YarnClusterStats {

  def withContainer(containerInfo: ContainerInfo): YarnClusterStats

  def getContainerInfo(containerId: ContainerId): Option[ContainerInfo]

  def getRunningContainersCount(containerType: ContainerType): Int

  def withUpdatedContainer(newStatus: ContainerStatus): Option[YarnClusterStats]

  def getCompletedContainersCount(containerType: ContainerType): Int

  def getRunningMasterContainersAddrs: Array[String]

  def containerStats: ContainerStats
}

final case class ImmutableYarnClusterStats(containers: Map[ContainerId, ContainerInfo],
                                  masterContainersRequested: Int,
                                  workerContainersRequested: Int) extends YarnClusterStats {

  private val LOG: Logger = LogUtil.getLogger(getClass)

  def withContainer(containerInfo: ContainerInfo): ImmutableYarnClusterStats = {
    this.copy(containers = containers + (containerInfo.status.getContainerId -> containerInfo))
  }

  def getContainerInfo(containerId: ContainerId): Option[ContainerInfo] = {
    containers.get(containerId)
  }

  def getRunningContainersCount(containerType: ContainerType): Int = {
    filterContainersByType(containerType).count(_._2.status.getState == ContainerState.RUNNING)
  }

  def withUpdatedContainer(newStatus: ContainerStatus):Option[ImmutableYarnClusterStats] = {
    containers.get(newStatus.getContainerId) match {
      case Some(info) => Some(withContainer(info.copy(status = newStatus)))
      case None => None
    }
  }

  def getCompletedContainersCount(containerType: ContainerType):Int = {
    filterContainersByType(containerType).count(_._2.status.getState == ContainerState.COMPLETE)
  }

  def getRunningMasterContainersAddrs():Array[String] = {
    filterContainersByType(MASTER).filter(_._2.status.getState == ContainerState.RUNNING).map(pair => {
      val (_, info) = pair
      info.host + ":" + info.port
    }).toArray
  }

  def containerStats: ContainerStats = {
    containers.foldLeft(
      ContainerStats(0,0,0,masterContainersRequested + workerContainersRequested)
    )((last, next) => {
      val (_, info) = next
      ContainerStats(last.allocated+1,
        last.completed + completed(info),
        last.failed + failed(info),
        last.requested
      )
    })
  }

  private def filterContainersByType(containerType: ContainerType): Map[ContainerId, ContainerInfo] = {
    containers.filter(_._2.containerType == containerType)
  }

  private def completed(info: ContainerInfo):Int = {
    info.status.getState match {
      case ContainerState.COMPLETE =>
        info.status.getExitStatus match {
          case 0 =>
            1
          case _ =>
            0
        }
      case _ =>
        0
    }
  }

  /**
   * Every container but COMPLETE with exit status 0, are considered failed
   * @param info
   * @return
   */
  private def failed(info: ContainerInfo):Int = {
    info.status.getState match {
      case ContainerState.COMPLETE =>
        info.status.getExitStatus match {
          case 0 =>
            0
          case _ =>
            1
        }
      case _ =>
        1
    }
  }

}

object YarnClusterStats {
  def apply(masterContainersRequested:Int, workerContainersRequested:Int):YarnClusterStats = {
    new ImmutableYarnClusterStats(Map.empty[ContainerId, ContainerInfo],
                         masterContainersRequested,
                         workerContainersRequested)
  }
}