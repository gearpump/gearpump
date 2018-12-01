package io.gearpump.jarstore

import akka.actor.{Actor, Stash}
import akka.pattern.pipe
import io.gearpump.util.Constants
import io.gearpump.cluster.ClientToMaster.{GetJarStoreServer, JarStoreServerAddress}

class JarStoreServer(jarStoreRootPath: String) extends Actor with Stash {
  private val host = context.system.settings.config.getString(Constants.GEARPUMP_HOSTNAME)
  private val jarStore = JarStore.get(jarStoreRootPath)
  jarStore.init(context.system.settings.config)
  private val server = new FileServer(context.system, host, 0, jarStore)
  implicit val timeout = Constants.FUTURE_TIMEOUT
  implicit val executionContext = context.dispatcher

  server.start pipeTo self

  def receive: Receive = {
    case FileServer.Port(port) =>
      context.become(listen(port))
      unstashAll()
    case _ =>
      stash()
  }

  def listen(port: Int): Receive = {
    case GetJarStoreServer =>
      sender ! JarStoreServerAddress(s"http://$host:$port/")
  }

  override def postStop(): Unit = {
    server.stop
  }
}
