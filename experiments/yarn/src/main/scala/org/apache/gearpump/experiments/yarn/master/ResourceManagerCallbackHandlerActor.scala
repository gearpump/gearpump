package org.apache.gearpump.experiments.yarn.master

import org.apache.gearpump.util.LogUtil
import akka.actor._
import org.apache.gearpump.experiments.yarn.AppConfig

class ResourceManagerCallbackHandlerActor(appConfig: AppConfig, yarnAM: ActorRef) extends Actor {
  val LOG = LogUtil.getLogger(getClass)
  val rmCallbackHandler = new ResourceManagerCallbackHandler(appConfig, yarnAM)

  override def preStart(): Unit = {
    LOG.info("Sending RMCallbackHandler to YarnAM")
    yarnAM ! rmCallbackHandler
  }

  override def receive: Receive = {
    case _ =>
      LOG.error(s"Unknown message received")
  }

}
