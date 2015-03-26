/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.cluster.main

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigValueFactory
import org.apache.gearpump.cluster.ClusterConfig
import org.apache.gearpump.cluster.master.{Master => MasterActor}
import org.apache.gearpump.cluster.worker.{Worker => WorkerActor}
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.LogUtil.ProcessType
import org.apache.gearpump.util.{ActorUtil, LogUtil}
import org.slf4j.Logger
import org.apache.gearpump.cluster

import scala.collection.JavaConverters._

object Local extends App with ArgumentsParser {
  val systemConfig = ClusterConfig.load.master.withFallback(ClusterConfig.load.worker)

  private val LOG: Logger = {
    LogUtil.loadConfiguration(systemConfig, ProcessType.LOCAL)
    LogUtil.getLogger(getClass)
  }

  override val options: Array[(String, CLIOption[Any])] =
    Array("ip"->CLIOption[String]("<master ip address>",required = false, defaultValue = Some("127.0.0.1")),
          "port"->CLIOption("<master port>",required = true),
          "sameprocess" -> CLIOption[Boolean]("", required = false, defaultValue = Some(false)),
          "workernum"-> CLIOption[Int]("<how many workers to start>", required = false, defaultValue = Some(2)))



  def start() : Unit = {
    val config = parse(args)
    if (null == config) {
      return
    }

    local(config.getString("ip"),  config.getInt("port"), config.getInt("workernum"), config.getBoolean("sameprocess"))
  }

  def local(ip : String, port : Int, workerCount : Int, sameProcess : Boolean) : Unit = {
    if (sameProcess) {
      LOG.info("Starting local in same process")
      System.setProperty("LOCAL", "true")
    }

    implicit val system = ActorSystem(MASTER, systemConfig.
      withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(port)).
      withValue(NETTY_TCP_HOSTNAME, ConfigValueFactory.fromAnyRef(ip)).
      withValue(GEARPUMP_CLUSTER_MASTERS,  ConfigValueFactory.fromAnyRef(List(s"$ip:$port").asJava))
    )

    val master = system.actorOf(Props[MasterActor], MASTER)
    val masterPath = ActorUtil.getSystemAddress(system).toString + s"/user/$MASTER"
    LOG.info(s"master is started at $masterPath...")

    0.until(workerCount).foreach { id =>
      system.actorOf(Props(classOf[WorkerActor], master), classOf[WorkerActor].getSimpleName + id)
    }
  }

  start()
}
