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
package org.apache.gearpump.experiments.yarn.client

import java.io.{File, IOException, OutputStreamWriter}
import java.net.InetAddress
import java.util.zip.ZipInputStream

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.gearpump.cluster.ClusterConfig
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.experiments.yarn.Constants
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.gearpump.experiments.yarn.appmaster.AppMasterCommand
import org.apache.gearpump.experiments.yarn.appmaster.YarnAppMaster.{ActiveConfig, GetActiveConfig}
import org.apache.gearpump.experiments.yarn.glue.Records.{ApplicationId, Resource}
import org.apache.gearpump.experiments.yarn.glue.{FileSystem, YarnClient, YarnConfig}
import org.apache.gearpump.util.ActorUtil.askActor
import org.apache.gearpump.util.{AkkaApp, LogUtil, Util}
import org.slf4j.Logger

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
 * Launch Gearpump on YARN
 */
class LaunchCluster(
    akka: Config,
    yarnConf: YarnConfig,
    yarnClient: YarnClient,
    fs: FileSystem,
    actorSystem: ActorSystem,
    appMasterResolver: AppMasterResolver,
    version: String = Util.version) {

  import org.apache.gearpump.experiments.yarn.Constants._
  private implicit val dispatcher = actorSystem.dispatcher

  private val LOG: Logger = LogUtil.getLogger(getClass)
  private val host = InetAddress.getLocalHost.getHostName
  private val queue = akka.getString(APPMASTER_QUEUE)
  private val memory = akka.getString(APPMASTER_MEMORY).toInt
  private val vcore = akka.getString(APPMASTER_VCORES).toInt

  def submit(appName: String, packagePath: String): ApplicationId = {
    LOG.info("Starting AM")

    // First step, check the version, to make sure local version matches remote version
    if (!packagePath.endsWith(".zip")) {
      throw new IOException(s"YarnClient only support .zip distribution package," +
        s" now it is ${packagePath}. Please download the zip " +
        "package from website or use sbt assembly packArchiveZip to build one.")
    }

    if (!fs.exists(packagePath)) {
      throw new IOException(s"Cannot find package ${packagePath} on HDFS ${fs.name}. ")
    }

    val rootEntry = rootEntryPath(zip = packagePath)

    if (!rootEntry.contains(version)) {
      throw new IOException(s"Check version failed! Local gearpump binary" +
        s" version $version doesn't match with remote path $packagePath")
    }

    val resource = Resource.newInstance(memory, vcore)
    val appId = yarnClient.createApplication

    // uploads the configs to HDFS home directory of current user.
    val configPath = uploadConfigToHDFS(appId)

    val command = AppMasterCommand(akka, rootEntry, Array(s"-conf $configPath",
      s"-package $packagePath"))

    yarnClient.submit(appName, appId, command.get, resource, queue, packagePath, configPath)

    LOG.info("Waiting application to finish...")
    val report = yarnClient.awaitApplication(appId, LaunchCluster.TIMEOUT_MILLISECONDS)
    LOG.info(s"Application $appId finished with state ${report.getYarnApplicationState} " +
      s"at ${report.getFinishTime}, info: ${report.getDiagnostics}")

    // scalastyle:off println
    Console.println("================================================")
    Console.println("==Application Id: " + appId)
    // scalastyle:on println
    appId
  }

  def saveConfig(appId: ApplicationId, output: String): Future[File] = {
    LOG.info(s"Trying to download active configuration to output path: " + output)
    LOG.info(s"Resolving YarnAppMaster ActorRef for application " + appId)
    val appMaster = appMasterResolver.resolve(appId)
    LOG.info(s"appMaster=${appMaster.path} host=$host")
    val future = askActor[ActiveConfig](appMaster, GetActiveConfig(host)).map(_.config)
    future.map { config =>
      val out = new File(output)
      ClusterConfig.saveConfig(config, out)
      out
    }
  }

  private def uploadConfigToHDFS(appId: ApplicationId): String = {
    // Uses personal home directory so that it will not conflict with other users
    // conf path pattern: /user/<userid>/.gearpump_application_<timestamp>_<id>/conf
    val confDir = s"${fs.getHomeDirectory}/.gearpump_${appId}/conf/"
    LOG.info(s"Uploading configuration files to remote HDFS(under $confDir)...")

    // Copies config from local to remote.
    val remoteConfFile = s"$confDir/gear.conf"
    var out = fs.create(remoteConfFile)
    var writer = new OutputStreamWriter(out)

    val cleanedConfig = ClusterConfig.filterOutDefaultConfig(akka)

    writer.write(cleanedConfig.root().render())
    writer.close()

    // Saves yarn-site.xml to remote
    val yarn_site_xml = s"$confDir/yarn-site.xml"
    out = fs.create(yarn_site_xml)
    writer = new OutputStreamWriter(out)
    yarnConf.writeXml(writer)
    writer.close()

    // Saves log4j.properties to remote
    val log4j_properties = s"$confDir/log4j.properties"
    val log4j = LogUtil.loadConfiguration
    out = fs.create(log4j_properties)
    writer = new OutputStreamWriter(out)
    log4j.store(writer, "gearpump on yarn")
    writer.close()
    confDir.toString
  }

  private def rootEntryPath(zip: String): String = {
    val stream = new ZipInputStream(fs.open(zip))
    val entry = stream.getNextEntry()
    val name = entry.getName
    name.substring(0, entry.getName.indexOf("/"))
  }
}

object LaunchCluster extends AkkaApp with ArgumentsParser {

  val PACKAGE = "package"
  val NAME = "name"
  val VERBOSE = "verbose"
  val OUTPUT = "output"

  override protected def akkaConfig: Config = {
    ClusterConfig.default()
  }

  override val options: Array[(String, CLIOption[Any])] = Array(
    PACKAGE -> CLIOption[String]("<Please specify the gearpump.zip package path on HDFS. " +
      "If not specified, we will use default value /user/gearpump/gearpump.zip>", required = false),
    NAME -> CLIOption[String]("<Application name showed in YARN>", required = false,
      defaultValue = Some("Gearpump")),
    VERBOSE -> CLIOption("<print verbose log on console>", required = false,
      defaultValue = Some(false)),
    OUTPUT -> CLIOption("<output path for configuration file>", required = false,
      defaultValue = None)
  )
  private val TIMEOUT_MILLISECONDS = 30 * 1000

  override def main(inputAkkaConf: Config, args: Array[String]): Unit = {
    val parsed = parse(args)
    if (parsed.getBoolean(VERBOSE)) {
      LogUtil.verboseLogToConsole()
    }
    val userName = inputAkkaConf.getString(CONTAINER_USER)
    if (userName != null) {
      System.setProperty("HADOOP_USER_NAME", userName)
    }

    val yarnConfig = new YarnConfig()
    val fs = new FileSystem(yarnConfig)
    val yarnClient = new YarnClient(yarnConfig)
    val akkaConf = updateConf(inputAkkaConf, parsed)
    val actorSystem = ActorSystem("launchCluster", akkaConf)
    val appMasterResolver = new AppMasterResolver(yarnClient, actorSystem)

    val client = new LaunchCluster(akkaConf, yarnConfig, yarnClient, fs,
      actorSystem, appMasterResolver)

    val name = parsed.getString(NAME)
    val appId = client.submit(name, akkaConf.getString(Constants.PACKAGE_PATH))

    if (parsed.exists(OUTPUT)) {
      import scala.concurrent.duration._
      Await.result(client.saveConfig(appId, parsed.getString(OUTPUT)),
        TIMEOUT_MILLISECONDS.milliseconds)
    }

    yarnClient.stop()
    actorSystem.terminate()
    Await.result(actorSystem.whenTerminated, Duration.Inf)
  }

  private def updateConf(akka: Config, parsed: ParseResult): Config = {
    if (parsed.exists(PACKAGE)) {
      akka.withValue(Constants.PACKAGE_PATH,
        ConfigValueFactory.fromAnyRef(parsed.getString(PACKAGE)))
    } else {
      akka
    }
  }
}