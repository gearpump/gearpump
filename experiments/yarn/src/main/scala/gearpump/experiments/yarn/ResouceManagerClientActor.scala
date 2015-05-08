package gearpump.experiments.yarn

import gearpump.experiments.yarn.master.{ResourceManagerCallbackHandler, YarnApplicationMaster}
import Actions.AMStatusMessage
import Actions.ContainerRequestMessage
import Actions.RegisterAMMessage
import gearpump.util.LogUtil
import org.apache.hadoop.yarn.api.records.Priority
import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import akka.actor.Actor
import akka.actor.ActorRef

class ResourceManagerClientActor(yarnConf: YarnConfiguration, yarnAM: ActorRef) extends Actor {
  val LOG = LogUtil.getLogger(getClass)
  var client: AMRMClientAsync[ContainerRequest] = _
  
  override def receive: Receive = {
    case rmCallbackHandler: ResourceManagerCallbackHandler =>
      LOG.info("Received RMCallbackHandler")
      client = start(rmCallbackHandler)
    case containerRequest: ContainerRequestMessage =>
      LOG.info("Received ContainerRequestMessage")
      client.addContainerRequest(createContainerRequest(containerRequest))
    case amAttr: RegisterAMMessage =>
      LOG.info(s"Received RegisterAMMessage! ${amAttr.appHostName}:${amAttr.appHostPort}${amAttr.appTrackingUrl}")
      val response = client.registerApplicationMaster(amAttr.appHostName, amAttr.appHostPort, amAttr.appTrackingUrl)
      LOG.info("got response : " + response)
      yarnAM ! response
    case amStatus: AMStatusMessage =>
      LOG.info("Received AMStatusMessage")
      client.unregisterApplicationMaster(amStatus.appStatus, amStatus.appMessage, amStatus.appTrackingUrl)
  }

  private[this] def createContainerRequest(attrs: ContainerRequestMessage): ContainerRequest = {
    LOG.info("creating ContainerRequest")
    val priorityRecord = Records.newRecord(classOf[Priority])
    priorityRecord.setPriority(0)
    val priority = Priority.newInstance(0)
    val capability = Resource.newInstance(attrs.memory, attrs.vCores)
    new ContainerRequest(capability, null, null, priority)
  }

  private[this] def start(rmCallbackHandler: ResourceManagerCallbackHandler): AMRMClientAsync[ContainerRequest] = {
    LOG.info("starting AMRMClientAsync")
    import YarnApplicationMaster._
    val amrmClient: AMRMClientAsync[ContainerRequest] = AMRMClientAsync.createAMRMClientAsync(TIME_INTERVAL, rmCallbackHandler)
    amrmClient.init(yarnConf)
    amrmClient.start()
    amrmClient
  }

  override def preStart(): Unit = {
    LOG.info("preStart")
  }
}