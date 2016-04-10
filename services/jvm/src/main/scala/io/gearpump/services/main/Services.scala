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

package io.gearpump.services.main

import scala.collection.JavaConverters._
import scala.concurrent.Await


import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigValueFactory
import org.slf4j.Logger

import io.gearpump.cluster.ClusterConfig
import io.gearpump.cluster.main.{ArgumentsParser, CLIOption, Gear}
import io.gearpump.cluster.master.MasterProxy
import io.gearpump.services.{RestServices, SecurityService}
import io.gearpump.util.LogUtil.ProcessType
import io.gearpump.util.{AkkaApp, Constants, LogUtil, Util}

object Services extends AkkaApp with ArgumentsParser {

  private val LOG = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption("<host:port>", required = false),
    Gear.OPTION_CONFIG -> CLIOption("<provide a custom configuration file>", required = false),
    "supervisor" -> CLIOption("<Supervisor Actor Path>", required = false, Some("")))

  override val description = "UI Server"

  override def akkaConfig: Config = {
    ClusterConfig.ui()
  }

  override def help(): Unit = {
    // scalastyle:off println
    Console.println("UI Server")
    // scalastyle:on println
  }

  private var killFunction: Option[() => Unit] = None

  override def main(inputAkkaConf: Config, args: Array[String]): Unit = {

    val argConfig = parse(args)
    var akkaConf =
      if (argConfig.exists(Gear.OPTION_CONFIG)) {
        ClusterConfig.ui(argConfig.getString(Gear.OPTION_CONFIG))
      } else {
        inputAkkaConf
      }

    val LOG: Logger = {
      LogUtil.loadConfiguration(akkaConf, ProcessType.UI)
      LogUtil.getLogger(getClass)
    }

    if (argConfig.exists("master")) {
      val master = argConfig.getString("master")
      akkaConf = akkaConf.withValue(Constants.GEARPUMP_CLUSTER_MASTERS,
        ConfigValueFactory.fromIterable(List(master).asJava))
    }

    akkaConf = akkaConf.withValue(Constants.GEARPUMP_SERVICE_SUPERVISOR_PATH,
      ConfigValueFactory.fromAnyRef(argConfig.getString("supervisor")))
      // Create a random unique secret key for session manager.
      // All previous stored session token cookies will be invalidated when UI
      // server is restarted.
      .withValue(SecurityService.SESSION_MANAGER_KEY,
        ConfigValueFactory.fromAnyRef(java.util.UUID.randomUUID().toString()))

    val masterCluster = akkaConf.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).asScala
      .flatMap(Util.parseHostList)

    implicit val system = ActorSystem("services", akkaConf)
    implicit val executionContext = system.dispatcher

    import scala.concurrent.duration._
    val master = system.actorOf(MasterProxy.props(masterCluster, 1.day),
      s"masterproxy${system.name}")
    val (host, port) = parseHostPort(system.settings.config)


    implicit val mat = ActorMaterializer()
    val services = new RestServices(master, mat, system)

    val bindFuture = Http().bindAndHandle(Route.handlerFlow(services.route), host, port)
    Await.result(bindFuture, 15.seconds)

    val displayHost = if (host == "0.0.0.0") "127.0.0.1" else host
    LOG.info(s"Please browse to http://$displayHost:$port to see the web UI")

    // scalastyle:off println
    println(s"Please browse to http://$displayHost:$port to see the web UI")
    // scalastyle:on println

    killFunction = Some { () =>
      LOG.info("Shutting down UI Server")
      system.terminate()
    }

    Await.result(system.whenTerminated, Duration.Inf)
  }

  private def parseHostPort(config: Config): (String, Int) = {
    val port = config.getInt(Constants.GEARPUMP_SERVICE_HTTP)
    val host = config.getString(Constants.GEARPUMP_SERVICE_HOST)
    (host, port)
  }

  // TODO: fix this
  // Hack around for YARN module, so that we can kill the UI server
  // when application is shutting down.
  def kill(): Unit = {
    if (killFunction.isDefined) {
      killFunction.get.apply()
    }
  }
}