package gearpump.experiments.yarn

import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.api.records.ContainerId

object Actions {
  sealed trait Reason
  case class Failed(throwable: Throwable) extends Reason
  case object ShutdownRequest extends Reason
  case object AllRequestedContainersCompleted extends Reason
  
  sealed trait ContainerType
  case object Master extends ContainerType
  case object Woker extends ContainerType
  case object Service extends ContainerType

  sealed trait YarnApplicationMasterState
  case object RequestingMasters extends YarnApplicationMasterState
  case object RequestingWorkers extends YarnApplicationMasterState
  case object RequestingService extends YarnApplicationMasterState
  
  sealed trait YarnApplicationMasterData
  case object Uninitialized extends YarnApplicationMasterData
  
  case class LaunchContainers(containers: List[Container]) extends YarnApplicationMasterData 
  case class LaunchWorkerContainers(containers: List[Container])
  case class LaunchServiceContainer(containers: List[Container])
  case class ContainerRequestMessage(memory: Int, vCores: Int)
  case class RMHandlerDone(reason: Reason, rMHandlerContainerStats: RMHandlerContainerStats)
  case class RMHandlerContainerStats(allocated: Int, completed: Int, failed: Int)
  case class RegisterAMMessage(appHostName: String, appHostPort: Int, appTrackingUrl: String)
  case class AMStatusMessage(appStatus: FinalApplicationStatus, appMessage: String, appTrackingUrl: String)
  case class ContainerInfo(container:Container, containerType: ContainerType, launchCommand: String => String)
  case class ContainerStarted(containerId: ContainerId)
}
