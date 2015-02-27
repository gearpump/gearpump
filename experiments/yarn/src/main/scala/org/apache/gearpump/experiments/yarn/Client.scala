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

import java.io._
import java.nio.ByteBuffer

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.ApplicationConstants
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


trait ClientAPI {
  def configureAMLaunchContext: ContainerLaunchContext
  def getConfiguration: AppConfig
  def getCommand: String
  def getYarnConf: YarnConfiguration
  def getAppEnv: Map[String, String]
  def getAMCapability: Resource
  def getAMLocalResourcesMap: Map[String, LocalResource]
  def monitorAM(appContext: ApplicationSubmissionContext): Unit
  def uploadAMResourcesToHDFS(): Unit
}
//def getEnvVars(conf: Config)(key: String): String

//cliopts: ParseResult, conf: Config
class Client(configuration:AppConfig, yarnConf: YarnConfiguration, yarnClient: YarnClient) extends ClientAPI {
  import org.apache.gearpump.experiments.yarn.Client._
  import org.apache.gearpump.experiments.yarn.EnvVars._

  val LOG: Logger = LogUtil.getLogger(getClass)
  def getConfiguration = configuration
  def getEnv = getConfiguration.getEnv _
  def getYarnConf = yarnConf
  def getFs = FileSystem.get(getYarnConf)  
  def getHdfs = new Path(getFs.getHomeDirectory, getEnv(HDFS_PATH))
  

  private[this] def getMemory(envVar: String): Int = {
    val memory = getEnv(envVar).trim
    val containerMemory = memory.substring(0, memory.length-1).toInt
    val memoryUnits = memory.last.toUpper match {
      case 'G' =>
        containerMemory*1024
      case 'M' =>
        containerMemory
      case _ =>
        LOG.error(s"Invalid value $memory defaulting to 2G")
        2048
    }
    containerMemory
  }

  def getCommand: String = {
    val exe = getEnv(YARNAPPMASTER_COMMAND)
    val mainClass = getEnv(YARNAPPMASTER_MAIN)
    val count = getEnv(CONTAINER_COUNT).trim
    val memory = getMemory(CONTAINER_MEMORY)
    val vmcores = getEnv(CONTAINER_VCORES)
    val logdir = "/tmp" //ApplicationConstants.LOG_DIR_EXPANSION_VAR
    val command = s"$exe $mainClass -containers $count -containerMemory $memory -containerVCores $vmcores 1>$logdir/AM_stdout 2>$logdir/AM_stderr"
    LOG.info(s"command=$command")
    command
  }

 
  def getAppEnv: Map[String, String] = {
    val appMasterEnv = new java.util.HashMap[String,String]
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
    val excludeJars = getEnv(EXCLUDE_JARS).split("\\,").map(excludeJar => {
      excludeJar.trim
    }).toIndexedSeq
    LOG.info(s"jarDir=$jarDir")
    Option(new File(jarDir)).foreach(_.list.filter(file => {
      file.endsWith(".jar")
    }).filter(jar => {
      !excludeJars.contains(jar)
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
    amContainer.setCommands(Seq(getCommand))
    val environment = getAppEnv
    environment.foreach(pair => {
      val (key, value) = pair
      LOG.info(s"getAppEnv key=$key value=$value")
    })
    amContainer.setEnvironment(getAppEnv)
    amContainer.setLocalResources(getAMLocalResourcesMap)
    val credentials = UserGroupInformation.getCurrentUser.getCredentials
    val dob = new DataOutputBuffer
    credentials.writeTokenStorageToStream(dob)
    amContainer.setTokens(ByteBuffer.wrap(dob.getData))
    amContainer
  }

  def getAMCapability: Resource = {
    val capability = Records.newRecord(classOf[Resource])
    val memory = getEnv(YARNAPPMASTER_MEMORY).trim
    val containerMemory = memory.substring(0, memory.length-1).toInt
    val memoryUnits = memory.last.toUpper match {
      case 'G' =>
        containerMemory*1024
      case 'M' =>
        containerMemory
      case _ =>
        LOG.error(s"Invalid value $memory defaulting to 2G")
        2048
    }
    capability.setMemory(containerMemory)
    capability.setVirtualCores(getEnv(YARNAPPMASTER_VCORES).toInt)
    capability
  }

  def getAMLocalResourcesMap: Map[String, LocalResource] = {
    getFs.listStatus(getHdfs).map(fileStatus => {
      val localResouceFile = Records.newRecord(classOf[LocalResource])
      val path = ConverterUtils.getYarnUrlFromPath(fileStatus.getPath)
      LOG.info(s"local resource path=${path.getFile}")
      localResouceFile.setResource(path)
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
    appContext.setApplicationName(getEnv(YARNAPPMASTER_NAME))
    appContext.setAMContainerSpec(configureAMLaunchContext)
    appContext.setResource(getAMCapability)
    appContext.setQueue(getEnv(YARNAPPMASTER_QUEUE))
    yarnClient.submitApplication(appContext)
    monitorAM(appContext)
  }

  deploy()
}

object Client extends App with ArgumentsParser {
  import org.apache.gearpump.experiments.yarn.EnvVars._
  
  case class ClusterResources(totalFreeMemory: Long, totalContainers: Int, nodeManagersFreeMemory: Map[String, Long])

  override val options: Array[(String, CLIOption[Any])] = Array(
    "jars" -> CLIOption[String]("<AppMaster jar directory>", required = false),
    "main" -> CLIOption[String]("<AppMaster main class>", required = false),
    "monitor" -> CLIOption[Boolean]("<monitor AppMaster state>", required = false, defaultValue = Some(false))
  )

  new Client(new AppConfig(parse(args), ConfigFactory.load), new YarnConfiguration, YarnClient.createYarnClient)
}
