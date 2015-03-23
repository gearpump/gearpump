package org.apache.gearpump.experiments.yarn.master

import java.net.InetAddress

import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.apache.gearpump.experiments.yarn.YarnContainerUtil
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.conf.YarnConfiguration
import akka.actor._

class ServicesLauncherActor(masters: Array[String], host: String, port: Int) extends Actor {
  val LOG = LogUtil.getLogger(getClass)  
  
  override def preStart(): Unit = {
    val mastersArg = masters.mkString(",")
    LOG.info(s"Launching services -master $mastersArg -host $host -httpPort $port")
    org.apache.gearpump.cluster.main.Services.main(Array("-master", mastersArg, "-host", host, "-httpPort", port.toString))
  }

  override def receive: Receive = {
    case _ =>
      LOG.error(s"Unknown message received")
  }  
}
