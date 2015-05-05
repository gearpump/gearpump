package org.apache.gearpump.experiments.yarn

import akka.actor.{Actor, ActorRef, actorRef2Scala}
import org.apache.gearpump.experiments.yarn.master.{AmActorProtocol, ResourceManagerCallbackHandler, YarnApplicationMaster}
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.yarn.api.records.{Priority, Resource}
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records

import scala.util.{Failure, Success, Try}

class ResourceManagerClient(yarnConf: YarnConfiguration, appConfig: AppConfig,
                            createHandler: Option[(AppConfig, ActorRef) => ResourceManagerCallbackHandler],
                            startClient: Option[(ResourceManagerCallbackHandler) =>  AMRMClientAsync[ContainerRequest]]) extends Actor {
  import AmActorProtocol._
  val LOG = LogUtil.getLogger(getClass)
  val callbackHandler: ResourceManagerCallbackHandler = createHandler match {
    case Some(create) =>
      create(appConfig, self)
    case None =>
      new ResourceManagerCallbackHandler(appConfig, self)
  }
  val applicationMaster: ActorRef = context.parent
  val client: Option[AMRMClientAsync[ContainerRequest]] = Try({
    startClient match {
      case Some(start) =>
        start(callbackHandler)
      case None =>
        start(callbackHandler)
    }
  }) match {
    case Success(asyncClient) =>
      callbackHandler.resourceManagerClient ! RMConnected
      Some(asyncClient)
    case Failure(throwable) =>
      callbackHandler.resourceManagerClient ! RMConnectionFailed(throwable)
      None
  }
  
  override def receive: Receive = {
    case RMConnected =>
      applicationMaster ! RMConnected

    case failed: RMConnectionFailed =>
      applicationMaster ! failed

    case additionalContainers@AdditionalContainersRequest(count) =>
      LOG.info("Received AdditionalContainersRequest for $count")
      applicationMaster ! additionalContainers

    case containersAllocated: ContainersAllocated =>
      applicationMaster ! containersAllocated

    case rmAllRequestedContainersCompleted: RMAllRequestedContainersCompleted =>
      applicationMaster ! rmAllRequestedContainersCompleted

    case rmError: RMError =>
      applicationMaster ! rmError

    case rmShutdownRequest: RMShutdownRequest =>
      applicationMaster ! rmShutdownRequest

    case containersRequest: ContainersRequest =>
      LOG.info("Received ContainersRequest")
      client.foreach(_.addContainerRequest(createContainerRequest(containersRequest)))

    case amAttr: RegisterAMMessage =>
      LOG.info(s"Received RegisterAMMessage! ${amAttr.appHostName}:${amAttr.appHostPort}${amAttr.appTrackingUrl}")
      val response = client.map(_.registerApplicationMaster(amAttr.appHostName, amAttr.appHostPort, amAttr.appTrackingUrl))
      response.foreach(sender ! RegisterAppMasterResponse(_))

    case amStatus: AMStatusMessage =>
      LOG.info("Received AMStatusMessage")
      client.foreach(_.unregisterApplicationMaster(amStatus.appStatus, amStatus.appMessage, amStatus.appTrackingUrl))
  }

  def createContainerRequest(attrs: ContainersRequest): ContainerRequest = {
    LOG.info("creating ContainerRequest")
    val priorityRecord = Records.newRecord(classOf[Priority])
    priorityRecord.setPriority(0)
    val priority = Priority.newInstance(0)
    val capability = Resource.newInstance(attrs.memory, attrs.vCores)
    new ContainerRequest(capability, null, null, priority)
  }

  def start(rmCallbackHandler: ResourceManagerCallbackHandler): AMRMClientAsync[ContainerRequest] = {
    LOG.info("starting AMRMClientAsync")
    val amrmClient = AMRMClientAsync.createAMRMClientAsync[ContainerRequest](YarnApplicationMaster.TIME_INTERVAL, rmCallbackHandler)
    amrmClient.init(yarnConf)
    amrmClient.start()
    amrmClient
  }
}
