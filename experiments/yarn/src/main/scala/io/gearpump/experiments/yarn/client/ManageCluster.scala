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

package io.gearpump.experiments.yarn.client

import java.io.{File, IOException}
import java.net.InetAddress
import scala.concurrent.{Await, Future}

import akka.actor.{ActorRef, ActorSystem}

import io.gearpump.cluster.ClientToMaster.{AddWorker, CommandResult, RemoveWorker}
import io.gearpump.cluster.ClusterConfig
import io.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import io.gearpump.experiments.yarn.appmaster.YarnAppMaster.{ActiveConfig, ClusterInfo, GetActiveConfig, Kill, QueryClusterInfo, QueryVersion, Version}
import io.gearpump.experiments.yarn.glue.Records.ApplicationId
import io.gearpump.experiments.yarn.glue.{YarnClient, YarnConfig}
import io.gearpump.util.ActorUtil.askActor
import io.gearpump.util.{AkkaApp, LogUtil}

class ManageCluster(appId: ApplicationId, appMaster: ActorRef, system: ActorSystem) {
  import ManageCluster._

  private val host = InetAddress.getLocalHost.getHostName
  implicit val dispatcher = system.dispatcher

  def getConfig: Future[ActiveConfig] = askActor[ActiveConfig](appMaster, GetActiveConfig(host))
  def version: Future[Version] = askActor[Version](appMaster, QueryVersion)
  def addWorker(count: Int): Future[CommandResult] = {
    askActor[CommandResult](appMaster, AddWorker(count))
  }

  def removeWorker(worker: String): Future[CommandResult] = {
    askActor[CommandResult](appMaster, RemoveWorker(worker))
  }

  def shutdown: Future[CommandResult] = askActor[CommandResult](appMaster, Kill)
  def queryClusterInfo: Future[ClusterInfo] = askActor[ClusterInfo](appMaster, QueryClusterInfo)

  def command(command: String, parsed: ParseResult): Future[AnyRef] = {
    command match {
      case GET_CONFIG =>
        if (parsed.exists(OUTPUT)) {
          getConfig.map { conf =>
            ClusterConfig.saveConfig(conf.config, new File(parsed.getString(OUTPUT)))
            conf
          }
        } else {
          throw new IOException(s"Please specify -$OUTPUT option")
        }
      case ADD_WORKER =>
        val count = parsed.getString(COUNT).toInt
        addWorker(count)
      case REMOVE_WORKER =>
        val containerId = parsed.getString(CONTAINER)
        if (containerId == null || containerId.isEmpty) {
          throw new IOException(s"Please specify -$CONTAINER option")
        } else {
          removeWorker(containerId)
        }
      case KILL =>
        shutdown
      case QUERY =>
        queryClusterInfo
      case VERSION =>
        version
    }
  }
}

object ManageCluster extends AkkaApp with ArgumentsParser {
  val GET_CONFIG = "getconfig"
  val ADD_WORKER = "addworker"
  val REMOVE_WORKER = "removeworker"
  val KILL = "kill"
  val VERSION = "version"
  val QUERY = "query"
  val COMMAND = "command"
  val CONTAINER = "container"
  val OUTPUT = "output"
  val COUNT = "count"
  val APPID = "appid"
  val VERBOSE = "verbose"

  val commands = List(GET_CONFIG, ADD_WORKER, REMOVE_WORKER, KILL, VERSION, QUERY)

  import scala.concurrent.duration._
  val TIME_OUT_SECONDS = 30.seconds

  override val options: Array[(String, CLIOption[Any])] = Array(
    COMMAND -> CLIOption[String](s"<${commands.mkString("|")}>", required = true),
    APPID -> CLIOption[String]("<Application id, format: application_timestamp_id>",
    required = true),
    COUNT -> CLIOption("<how many instance to add or remove>", required = false,
      defaultValue = Some(1)),
    VERBOSE -> CLIOption("<print verbose log on console>", required = false,
      defaultValue = Some(false)),
    OUTPUT -> CLIOption("<output path for configuration file>", required = false,
      defaultValue = Some("")),
    CONTAINER -> CLIOption("<container id for master or worker>", required = false,
      defaultValue = Some(""))
  )

  override def main(akkaConf: Config, args: Array[String]): Unit = {

    val yarnConfig = new YarnConfig()
    val yarnClient = new YarnClient(yarnConfig)

    val parsed = parse(args)

    if (parsed.getBoolean(VERBOSE)) {
      LogUtil.verboseLogToConsole()
    }

    val appId = parseAppId(parsed.getString(APPID))
    val system = ActorSystem("manageCluster", akkaConf)

    val appMasterResolver = new AppMasterResolver(yarnClient, system)
    val appMaster = appMasterResolver.resolve(appId)

    implicit val dispatcher = system.dispatcher
    val manager = new ManageCluster(appId, appMaster, system)

    val command = parsed.getString(COMMAND)
    val result = manager.command(command, parsed)

    // scalastyle:off println
    Console.println(Await.result(result, TIME_OUT_SECONDS))
    // scalastyle:on println
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }

  def parseAppId(str: String): ApplicationId = {
    val parts = str.split("_")
    val timestamp = parts(1).toLong
    val id = parts(2).toInt
    ApplicationId.newInstance(timestamp, id)
  }
}