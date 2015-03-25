package org.apache.gearpump.experiments.yarn.master

import akka.actor._
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.util.LogUtil

class ServicesLauncherActor(masters: Array[String], host: String, port: Int) extends Actor {
  val LOG = LogUtil.getLogger(getClass)  
  
  override def preStart(): Unit = {
    val mastersArg = masters.mkString(",")
    LOG.info(s"Launching services -master $mastersArg")
    System.setProperty("gearpump.services.host", host)
    System.setProperty("gearpump.services.http", port.toString)
    System.setProperty("akka.remote.netty.tcp.hostname", host)
    ConfigFactory.invalidateCaches()
    org.apache.gearpump.cluster.main.Services.main(Array("-master", mastersArg))
  }

  override def receive: Receive = {
    case _ =>
      LOG.error(s"Unknown message received")
  }  
}
