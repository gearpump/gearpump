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

package io.gearpump.experiments.storm

import java.io.File

import akka.actor.{Actor, ActorSystem, Props}
import backtype.storm.Config
import backtype.storm.generated.{ClusterSummary, StormTopology, SupervisorSummary, TopologySummary}
import com.typesafe.config.ConfigValueFactory
import io.gearpump.cluster.UserConfig
import io.gearpump.cluster.client.ClientContext
import io.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import io.gearpump.experiments.storm.Commands.{GetClusterInfo, _}
import io.gearpump.experiments.storm.topology.GearpumpStormTopology
import io.gearpump.experiments.storm.util.{GraphBuilder, StormConstants}
import io.gearpump.streaming.StreamApplication
import io.gearpump.util.{AkkaApp, Constants, LogUtil, Util}

import scala.collection.JavaConverters._

object StormRunner extends AkkaApp with ArgumentsParser {
  override val options: Array[(String, CLIOption[Any])] = Array(
    "jar" -> CLIOption[String]("<storm jar>", required = true),
    "config" -> CLIOption[String]("<storm config path>", required = false),
    "verbose" -> CLIOption("<print verbose log on console>", required = false, defaultValue = Some(false)))

  override val remainArgs = Array("topology_name")

  override def main(inputAkkaConf: Config, args: Array[String]): Unit = {

    val akkaConf = addStormClassPath(inputAkkaConf)
    val config = parse(args)

    val verbose = config.getBoolean("verbose")
    if (verbose) {
      LogUtil.verboseLogToConsole
    }

    val jar = config.getString("jar")
    val topology = config.remainArgs(0)
    val stormArgs = config.remainArgs.drop(1)
    val stormConf = new File(config.getString("config"))

    val system = ActorSystem("storm", akkaConf)
    val clientContext = new ClientContext(akkaConf, system, null)

    val stormNimbus = system.actorOf(Props(new Handler(clientContext, jar)))
    val thriftServer = GearpumpThriftServer(stormNimbus)
    thriftServer.start()

    val stormOptions = Array("-Dstorm.options=" +
      s"${Config.NIMBUS_HOST}=127.0.0.1,${Config.NIMBUS_THRIFT_PORT}=${GearpumpThriftServer.THRIFT_PORT}",
      "-Dstorm.jar=" + jar,
      "-Dstorm.conf.file=" + stormConf.getName
    )

    val classPath = Array(System.getProperty("java.class.path"), stormConf.getParent, jar)
    val process = Util.startProcess(stormOptions, classPath, topology, stormArgs)

    // wait till the process exit
    val exit = process.exitValue()

    thriftServer.close()
    clientContext.close()
    system.shutdown()

    if (exit != 0) {
      throw new Exception(s"failed to submit jar, exit code $exit, error summary: ${process.logger.summary}")
    }
  }

  import Constants._
  private def addStormClassPath(config: Config): Config = {
    val storm = s"<${GEARPUMP_HOME}>/lib/storm/*"
    val appClassPath = s"$storm${File.pathSeparator}" + config.getString(GEARPUMP_APPMASTER_EXTRA_CLASSPATH)
    val executorClassPath = s"$storm${File.pathSeparator}" + config.getString(Constants.GEARPUMP_EXECUTOR_EXTRA_CLASSPATH)

    config.withValue(GEARPUMP_APPMASTER_EXTRA_CLASSPATH, ConfigValueFactory.fromAnyRef(appClassPath))
      .withValue(GEARPUMP_EXECUTOR_EXTRA_CLASSPATH, ConfigValueFactory.fromAnyRef(executorClassPath))
  }

  class Handler(clientContext: ClientContext, jar: String) extends Actor {
    private var applications = Map.empty[String, Int]
    private var topologies = Map.empty[String, StormTopology]
    private val LOG = LogUtil.getLogger(classOf[Handler])

    implicit val system = context.system

    def receive: Receive = {
      case Kill(name, option) =>
        topologies -= name
        clientContext.shutdown(applications.getOrElse(name, throw new RuntimeException(s"topology $name not found")))
        val appId = applications(name)
        applications -= name
        LOG.info(s"Killed topology $name")
        sender ! AppKilled(name, appId)
      case Submit(name, uploadedJarLocation, jsonConf, topology, options) =>
        topologies += name -> topology

        val gearpumpStormTopology = new GearpumpStormTopology(topology, jsonConf)
        val processorGraph = GraphBuilder.build(gearpumpStormTopology)
        val config = UserConfig.empty.withValue[StormTopology](StormConstants.STORM_TOPOLOGY, topology)
        val app = StreamApplication("storm", processorGraph, config)
        val appId = clientContext.submit(app, jar)
        applications += name -> appId
        LOG.info(s"Storm Application $appId submitted")
        sender ! AppSubmitted(name, appId)
      case GetClusterInfo =>
        val topologySummaryList = topologies.map { case (name, _) =>
          new TopologySummary(name, name, 0, 0, 0, 0, "")
        }.toSeq
        sender ! new ClusterSummary(List[SupervisorSummary]().asJava, 0, topologySummaryList.asJava)
      case GetTopology(id) =>
        sender ! topologies(id)
    }
  }
}
