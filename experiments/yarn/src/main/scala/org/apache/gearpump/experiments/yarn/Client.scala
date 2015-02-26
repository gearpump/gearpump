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

package org.apache.gearpump.experiments.yarn

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{Apps, ConverterUtils, Records}
import org.slf4j.Logger

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
Features for YARNClient
- [ ] Configuration file needs to indicate how many workers to allocate with possible locations
- [ ] Configuration file needs to specify minimum master ram, vmcore requirements
- [ ] Configuration file needs to specify minimum worker ram, vmcore requirements
- [ ] Configuration file should specify where in HDFS to place jars for appmaster and workers
- [ ] Client needs to use YARN cluster API to find best nodes to run Master(s)
- [ ] Client needs to use YARN cluster API to find best nodes to run Workers
 */

object EnvVars {
  val APPMASTER_NAME = "gearpump.yarn.applicationmaster.name"
  val APPMASTER_COMMAND = "gearpump.yarn.applicationmaster.command"
  val APPMASTER_QUEUE = "default"
  val APPMASTER_MASTER_MEMORY = "gearpump.yarn.applicationmaster.masterMemory"
  val APPMASTER_MASTER_VMCORES = "gearpump.yarn.applicationmaster.masterVMCores"
  val CONTAINER_COMMAND = "gearpump.yarn.container.command"
  val HDFS_PATH = "gearpump.yarn.client.hdfsPath"
  val JARS = "gearpump.yarn.client.jars"
  val MIN_WORKER_COUNT = "gearpump.yarn.applicationmaster.minWorkerCount"
}

trait ClientAPI {
  def configureAMLaunchContext: ContainerLaunchContext
  def getConf: Config
  def getYarnConf: YarnConfiguration
  def getEnvVars(conf: Config)(key: String): String
  def getAppEnv: Map[String, String]
  def getAMCapability: Resource
  def getAMLocalResourcesMap: Map[String, LocalResource]
  def monitorAM(appContext: ApplicationSubmissionContext): Unit
  def uploadAMResourcesToHDFS(): Unit
}

class Client(cliopts: ParseResult, conf: Config, yarnConf: YarnConfiguration, yarnClient: YarnClient) extends ClientAPI {
  import org.apache.gearpump.experiments.yarn.Client._
  import org.apache.gearpump.experiments.yarn.EnvVars._

  val LOG: Logger = LogUtil.getLogger(getClass)

  def getConf = conf
  def getYarnConf = yarnConf
  def getFs = FileSystem.get(getYarnConf)
  def getEnv = getEnvVars(getConf)_
  def getHdfs = new Path(getFs.getHomeDirectory, getEnv(HDFS_PATH))

  def getEnvVars(conf: Config)(key: String): String = {
    val option = key.split("\\.").last.toUpperCase
    Option(cliopts) match {
      case Some(cliopts) =>
        cliopts.exists(option) match {
          case true =>
            cliopts.getString(option)
          case false =>
            conf.getString(key)
        }
      case None =>
        conf.getString(key)
    }
  }

  def getAppEnv: Map[String, String] = {
    val appMasterEnv = Map.empty[String,String]
    for (
      c <- getYarnConf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH.mkString(","))
    ) {
      Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
        c.trim(), File.pathSeparator)
    }
    Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
      Environment.PWD.$()+File.separator+"*", File.pathSeparator)
    appMasterEnv.toMap
  }

  def uploadAMResourcesToHDFS(): Unit = {
    val jarDir = getEnv(JARS)
    LOG.info(s"jarDir=$jarDir")
    Option(new File(jarDir)).map(_.list.filter(file => {
      file.endsWith(".jar")
    }).toList.foreach(jarFile => {
      Try(getFs.copyFromLocalFile(false, true, new Path(jarDir, jarFile), getHdfs)) match {
        case Success(a) =>
          LOG.info(s"$jarFile uploaded to HDFS")
        case Failure(error) =>
          LOG.error(s"$jarFile could not be uploaded to HDFS ${error.getMessage}")
          None
      }
    }))
  }

  def configureAMLaunchContext: ContainerLaunchContext = {
    uploadAMResourcesToHDFS()
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setCommands(Seq(getEnv(APPMASTER_COMMAND)))
    amContainer.setEnvironment(getAppEnv)
    amContainer.setLocalResources(getAMLocalResourcesMap)
    amContainer
  }

  def getAMCapability: Resource = {
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(getEnv(APPMASTER_MASTER_MEMORY).toInt)
    capability.setVirtualCores(getEnv(APPMASTER_MASTER_VMCORES).toInt)
    capability
  }

  def getAMLocalResourcesMap: Map[String, LocalResource] = {
    getFs.listStatus(getHdfs).map(fileStatus => {
      val localResouceFile = Records.newRecord(classOf[LocalResource])
      localResouceFile.setResource(ConverterUtils.getYarnUrlFromPath(fileStatus.getPath))
      localResouceFile.setType(LocalResourceType.FILE)
      localResouceFile.setSize(fileStatus.getLen)
      localResouceFile.setTimestamp(fileStatus.getModificationTime)
      localResouceFile.setVisibility(LocalResourceVisibility.APPLICATION)
      fileStatus.getPath.getName -> localResouceFile
    }).toMap
  }
  
  def clusterResources: ClusterResources = {
    val nodes:Seq[NodeReport] = yarnClient.getNodeReports(NodeState.RUNNING)
    nodes.foldLeft(ClusterResources(0L, 0, Map.empty[String, Long]))((clusterResources, nodeReport) => {
      val resource = nodeReport.getCapability
      ClusterResources(clusterResources.totalFreeMemory+resource.getMemory,
        clusterResources.totalContainers+nodeReport.getNumContainers,
        clusterResources.nodeManagersFreeMemory+(nodeReport.getNodeId.getHost->resource.getMemory))
    })
  }

  def monitorAM(appContext: ApplicationSubmissionContext): Unit = {
    val appId = appContext.getApplicationId
    var appReport = yarnClient.getApplicationReport(appId)
    var appState = appReport.getYarnApplicationState
    while (appState != YarnApplicationState.FINISHED &&
      appState != YarnApplicationState.KILLED &&
      appState != YarnApplicationState.FAILED) {
      Thread.sleep(1000)
      appReport = yarnClient.getApplicationReport(appId)
      appState = appReport.getYarnApplicationState
    }

    LOG.info(
      "Application " + appId + " finished with" +
        " state " + appState +
        " at " + appReport.getFinishTime)

  }

  def deploy() = {
    yarnClient.init(yarnConf)
    yarnClient.start()
    val appContext = yarnClient.createApplication.getApplicationSubmissionContext
    appContext.setApplicationName(getEnv(APPMASTER_NAME))
    appContext.setAMContainerSpec(configureAMLaunchContext)
    appContext.setResource(getAMCapability)
    appContext.setQueue(getEnv(APPMASTER_QUEUE))
    yarnClient.submitApplication(appContext)
    monitorAM(appContext)
  }
  deploy()
}

object Client extends App with ArgumentsParser {
  case class ClusterResources(totalFreeMemory: Long, totalContainers: Int, nodeManagersFreeMemory: Map[String, Long])

  override val options: Array[(String, CLIOption[Any])] = Array(
    "jars" -> CLIOption[String]("<AppMaster jar directory>", required = false),
    "main" -> CLIOption[String]("<AppMaster main class>", required = false),
    "monitor" -> CLIOption[Boolean]("<monitor AppMaster state>", required = false, defaultValue = Some(false))
  )

  new Client(parse(args), ConfigFactory.load, new YarnConfiguration, YarnClient.createYarnClient)
}
