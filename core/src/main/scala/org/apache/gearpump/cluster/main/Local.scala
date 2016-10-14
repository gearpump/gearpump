/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.gearpump.util.{ActorUtil, AkkaApp, Constants, LogUtil, Util}
import org.slf4j.Logger

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Local extends AkkaApp with ArgumentsParser {
  override def akkaConfig: Config = ClusterConfig.master()

  var LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] =
    Array("sameprocess" -> CLIOption[Boolean]("", required = false, defaultValue = Some(false)),
      "workernum" -> CLIOption[Int]("<how many workers to start>", required = false,
        defaultValue = Some(2)))

  override val description = "Start a local cluster"

  def main(akkaConf: Config, args: Array[String]): Unit = {

    this.LOG = {
      LogUtil.loadConfiguration(akkaConf, ProcessType.LOCAL)
      LogUtil.getLogger(getClass)
    }

    val config = parse(args)
    if (null != config) {
      local(config.getInt("workernum"), config.getBoolean("sameprocess"), akkaConf)
    }
  }

  def local(workerCount: Int, sameProcess: Boolean, akkaConf: Config): Unit = {
    if (sameProcess) {
      LOG.info("Starting local in same process")
      System.setProperty("LOCAL", "true")
    }
    val masters = akkaConf.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS)
      .asScala.flatMap(Util.parseHostList)
    val local = akkaConf.getString(Constants.GEARPUMP_HOSTNAME)

    if (masters.size != 1 && masters.head.host != local) {
      LOG.error(s"The ${Constants.GEARPUMP_CLUSTER_MASTERS} is not match " +
        s"with ${Constants.GEARPUMP_HOSTNAME}")
    } else {

      val hostPort = masters.head
      implicit val system = ActorSystem(MASTER, akkaConf.
        withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(hostPort.port))
      )

      val master = system.actorOf(Props[MasterActor], MASTER)
      val masterPath = ActorUtil.getSystemAddress(system).toString + s"/user/$MASTER"

      0.until(workerCount).foreach { id =>
        system.actorOf(Props(classOf[WorkerActor], master), classOf[WorkerActor].getSimpleName + id)
      }

      Await.result(system.whenTerminated, Duration.Inf)
    }
  }
}
