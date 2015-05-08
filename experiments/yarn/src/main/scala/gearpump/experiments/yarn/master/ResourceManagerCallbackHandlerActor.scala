package gearpump.experiments.yarn.master

import gearpump.experiments.yarn.AppConfig
import gearpump.util.LogUtil
import akka.actor._

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
