package org.apache.gearpump.experiments.yarn

import java.nio.ByteBuffer

import org.apache.gearpump.experiments.yarn.master.AmActorProtocol
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.hadoop.yarn.api.records.ContainerStatus
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.slf4j.Logger

import akka.actor.ActorRef


class NodeManagerCallbackHandler(am: ActorRef) extends NMClientAsync.CallbackHandler {
  import AmActorProtocol.ContainerStarted

  val LOG = LogUtil.getLogger(getClass)
  def onContainerStarted(containerId: ContainerId, allServiceResponse: java.util.Map[String, ByteBuffer]) {
    LOG.info(s"Container started : $containerId, " + allServiceResponse)
      am ! ContainerStarted(containerId)
  }
  
  def onContainerStatusReceived(containerId: ContainerId, containerStatus: ContainerStatus) {
    LOG.info(s"Container status received : $containerId, status $containerStatus")
  }

  def onContainerStopped(containerId: ContainerId) {
    LOG.info(s"Container stopped : $containerId")
  }

  def onGetContainerStatusError(containerId: ContainerId, throwable: Throwable) {
    LOG.error(s"Container exception : $containerId", throwable)
  }

  def onStartContainerError(containerId: ContainerId, throwable: Throwable) {
    LOG.error(s"Container exception : $containerId", throwable)
  }

  def onStopContainerError(containerId: ContainerId, throwable: Throwable) {
    LOG.error(s"Container exception : $containerId", throwable)
  }
}

trait NodeManagerCallbackHandlerFactory {
  def newInstance(am: ActorRef) :NodeManagerCallbackHandler
}

object NodeManagerCallbackHandlerFactory  {
  val instance: NodeManagerCallbackHandlerFactory = new NodeManagerCallbackHandlerFactory {
      override def newInstance(am: ActorRef): NodeManagerCallbackHandler = new NodeManagerCallbackHandler(am)
  }
  def apply() = instance
}
