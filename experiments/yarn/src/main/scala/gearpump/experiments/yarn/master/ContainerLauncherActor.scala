package gearpump.experiments.yarn.master

import gearpump.experiments.yarn.{AppConfig, YarnContainerUtil}
import org.apache.hadoop.yarn.api.records.{ContainerLaunchContext, Container}
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import gearpump.util.LogUtil
import org.apache.hadoop.yarn.conf.YarnConfiguration
import akka.actor._

import scala.collection.JavaConversions._

class ContainerLauncherActor(container: Container, nodeManagerClient: NMClientAsync,  yarnConf: YarnConfiguration, command: String, appConfig: AppConfig) extends Actor {
  val LOG = LogUtil.getLogger(getClass)

  override def preStart(): Unit = {
    val containerContext: ContainerLaunchContext = YarnContainerUtil.getContainerContext(yarnConf, command)
    containerContext.setLocalResources(YarnContainerUtil.getAMLocalResourcesMap(yarnConf, appConfig))
    nodeManagerClient.startContainerAsync(container, containerContext)
  }

  override def receive: Receive = {
    case _ =>
      LOG.error(s"Unknown message received")
  }  
}
