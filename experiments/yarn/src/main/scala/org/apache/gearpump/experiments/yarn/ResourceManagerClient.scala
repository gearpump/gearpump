package org.apache.gearpump.experiments.yarn

import akka.actor._
import org.apache.gearpump.experiments.yarn.AppConfig
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.gearpump.experiments.yarn.master.{ResourceManagerCallbackHandler, AmActorProtocol, YarnApplicationMaster}
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.yarn.api.records.{FinalApplicationStatus, Priority, Resource}
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records

import scala.util.{Failure, Success, Try}

class ResourceManagerClient(yarnConf: YarnConfiguration, appConfig: AppConfig,
                            createHandler: (AppConfig, ActorRef) => ResourceManagerCallbackHandler,
                            startClient: (ResourceManagerCallbackHandler) =>  AMRMClientAsync[ContainerRequest]) extends Actor {
  import AmActorProtocol._

  private val LOG = LogUtil.getLogger(getClass)
  private val callbackHandler = createHandler(appConfig, self)
  private val applicationMaster: ActorRef = context.parent
  private val client: Option[AMRMClientAsync[ContainerRequest]] = Try({
    startClient(callbackHandler)
  }) match {
    case Success(asyncClient) =>
      callbackHandler.resourceManagerClient ! RMConnected
      Some(asyncClient)
    case Failure(throwable) =>
      callbackHandler.resourceManagerClient ! RMConnectionFailed(throwable)
      None
  }
  
  override def receive: Receive = connectionHandler orElse
    containerHandler orElse
    registerHandler orElse
    terminalStateHandler

  def connectionHandler: Receive = {
    case RMConnected =>
      applicationMaster ! RMConnected

    case failed: RMConnectionFailed =>
      applicationMaster ! failed
  }

  def containerHandler: Receive = {
    case additionalContainers@AdditionalContainersRequest(count) =>
      LOG.info("Received AdditionalContainersRequest for $count")
      applicationMaster ! additionalContainers

    case containersAllocated: ContainersAllocated =>
      applicationMaster ! containersAllocated

    case containersRequest: ContainersRequest =>
      LOG.info("Received ContainersRequest")
      client.foreach(_.addContainerRequest(createContainerRequest(containersRequest)))
  }

  def registerHandler: Receive = {
    case amAttr: RegisterAMMessage =>
      LOG.info(s"Received RegisterAMMessage! ${amAttr.appHostName}:${amAttr.appHostPort}${amAttr.appTrackingUrl}")
      val response = client.map(_.registerApplicationMaster(amAttr.appHostName, amAttr.appHostPort, amAttr.appTrackingUrl))
      response.foreach(sender ! RegisterAppMasterResponse(_))
  }

  def terminalStateHandler: Receive = {
    case rmAllRequestedContainersCompleted@RMAllRequestedContainersCompleted(stats) =>
      val message = s"Diagnostics. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
      client.foreach(_.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, message, null))
      applicationMaster ! rmAllRequestedContainersCompleted

    case rmError@RMError(throwable, stats) =>
      LOG.info("Failed", throwable.getMessage)
      val message = s"Failed. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
      client.foreach(_.unregisterApplicationMaster(FinalApplicationStatus.FAILED, message, null))
      applicationMaster ! rmError

    case rmShutdownRequest@RMShutdownRequest(stats) =>
      if (stats.failed == 0 && stats.completed == appConfig.getEnv(WORKER_CONTAINERS).toInt) {
        val message = s"ShutdownRequest. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
        client.foreach(_.unregisterApplicationMaster(FinalApplicationStatus.KILLED, message, null))
      } else {
        val message = s"ShutdownRequest. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
        client.foreach(_.unregisterApplicationMaster(FinalApplicationStatus.FAILED, message, null))
      }
      applicationMaster ! rmShutdownRequest
  }

  def createContainerRequest(attrs: ContainersRequest): ContainerRequest = {
    LOG.info("creating ContainerRequest")
    val priorityRecord = Records.newRecord(classOf[Priority])
    priorityRecord.setPriority(0)
    val priority = Priority.newInstance(0)
    val capability = Resource.newInstance(attrs.memory, attrs.vCores)
    new ContainerRequest(capability, null, null, priority)
  }


}

object ResourceManagerClient {
  def props(yarnConf: YarnConfiguration, appConfig: AppConfig): Props = {

    def startAMRMClient(rmCallbackHandler: ResourceManagerCallbackHandler): AMRMClientAsync[ContainerRequest] = {
      val amrmClient = AMRMClientAsync.createAMRMClientAsync[ContainerRequest](YarnApplicationMaster.TIME_INTERVAL, rmCallbackHandler)
      amrmClient.init(yarnConf)
      amrmClient.start()
      amrmClient
    }

    Props(new ResourceManagerClient(yarnConf, appConfig,
      (appConfig: AppConfig, actorRef:ActorRef) => new ResourceManagerCallbackHandler(appConfig, actorRef),
      (rmCallbackHandler: ResourceManagerCallbackHandler) => startAMRMClient(rmCallbackHandler)
    ))
  }

  def props(yarnConf: YarnConfiguration, appConfig: AppConfig,
            createHandler: (AppConfig, ActorRef) => ResourceManagerCallbackHandler,
            startClient: (ResourceManagerCallbackHandler) =>  AMRMClientAsync[ContainerRequest]): Props =
  Props(new ResourceManagerClient(yarnConf, appConfig, createHandler, startClient))

}