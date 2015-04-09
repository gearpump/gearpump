package org.apache.gearpump.experiments.yarn.master

import akka.actor._
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.util.{Constants, LogUtil}

class ServicesLauncherActor(masters: Array[String], host: String, port: Int) extends Actor {
  val LOG = LogUtil.getLogger(getClass)  
  
  override def preStart(): Unit = {
    val mastersArg = masters.mkString(",")
    LOG.info(s"Launching services -master $mastersArg")
    System.setProperty("gearpump.services.host", host)
    System.setProperty("gearpump.services.http", port.toString)
    System.setProperty("akka.remote.netty.tcp.hostname", host)

    System.setProperty(Constants.GEARPUMP_HOSTNAME, host)
    for (index <- 0 until masters.length) {
      val masterHostPort = masters(index)
       System.setProperty(s"${Constants.GEARPUMP_CLUSTER_MASTERS}.$index", masterHostPort)
    }

    ConfigFactory.invalidateCaches()
    org.apache.gearpump.cluster.main.Services.main(Array.empty[String])
  }

  override def receive: Receive = {
    case _ =>
      LOG.error(s"Unknown message received")
  }  
}
