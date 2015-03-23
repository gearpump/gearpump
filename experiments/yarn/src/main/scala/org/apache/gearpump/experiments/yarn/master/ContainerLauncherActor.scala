package org.apache.gearpump.experiments.yarn.master

import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.apache.gearpump.experiments.yarn.YarnContainerUtil
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.conf.YarnConfiguration
import akka.actor._

class ContainerLauncherActor(container: Container, nodeManagerClient: NMClientAsync,  yarnConf: YarnConfiguration, command: String, version: String) extends Actor {
  val LOG = LogUtil.getLogger(getClass)  
  
  override def preStart(): Unit = {
    nodeManagerClient.startContainerAsync(container, YarnContainerUtil.getContainerContext(yarnConf, version, command))
  }

  override def receive: Receive = {
    case _ =>
      LOG.error(s"Unknown message received")
  }  
}
