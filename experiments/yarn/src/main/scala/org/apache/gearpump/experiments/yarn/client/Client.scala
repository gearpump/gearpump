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
package org.apache.gearpump.experiments.yarn.client

import java.io._
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{Apps, Records}
import org.slf4j.Logger
import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}
import org.apache.gearpump.experiments.yarn.AppConfig
import org.apache.gearpump.experiments.yarn.Constants.EXCLUDE_JARS
import org.apache.gearpump.experiments.yarn.Constants.HDFS_PATH
import org.apache.gearpump.experiments.yarn.Constants.JARS
import org.apache.gearpump.experiments.yarn.Constants.YARNAPPMASTER_COMMAND
import org.apache.gearpump.experiments.yarn.Constants.YARNAPPMASTER_MAIN
import org.apache.gearpump.experiments.yarn.Constants.YARNAPPMASTER_MEMORY
import org.apache.gearpump.experiments.yarn.Constants.YARNAPPMASTER_NAME
import org.apache.gearpump.experiments.yarn.Constants.YARNAPPMASTER_QUEUE
import org.apache.gearpump.experiments.yarn.Constants.YARNAPPMASTER_VCORES
import org.apache.gearpump.experiments.yarn.YarnContainerUtil


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
  def getConfiguration: AppConfig
  def getCommand: String
  def getYarnConf: YarnConfiguration
  def getAppEnv: Map[String, String]
  def getAMCapability: Resource
  def monitorAM(appContext: ApplicationSubmissionContext): Unit
  def uploadAMResourcesToHDFS(): Unit
}

class Client(configuration:AppConfig, yarnConf: YarnConfiguration, yarnClient: YarnClient) extends ClientAPI {
  import org.apache.gearpump.experiments.yarn.client.Client._
  import org.apache.gearpump.experiments.yarn.Constants._

  val LOG: Logger = LogUtil.getLogger(getClass)
  def getConfiguration = configuration
  def getEnv = getConfiguration.getEnv _
  def getYarnConf = yarnConf
  def getFs = FileSystem.get(getYarnConf)  
  def getHdfs = new Path(getFs.getHomeDirectory, getEnv(HDFS_PATH))

  val version = configuration.getEnv("version")

  private[this] def getMemory(envVar: String): Int = {
    val memory = getEnv(envVar).trim
    val containerMemory = memory.substring(0, memory.length-1).toInt
    val memoryUnits = memory.last.toUpper match {
      case 'G' =>
        containerMemory*1024
      case 'M' =>
        containerMemory
      case _ =>
        containerMemory
    }
    memoryUnits
  }

  def getCommand = {
    val exe = getEnv(YARNAPPMASTER_COMMAND)

    val classPath = Array(s"pack/$version/conf", s"pack/$version/dashboard", s"pack/$version/lib/*")
    val mainClass = getEnv(YARNAPPMASTER_MAIN)
    val logdir = ApplicationConstants.LOG_DIR_EXPANSION_VAR
    val command = s"$exe  -cp ${classPath.mkString(File.pathSeparator)}${File.pathSeparator}" +
      "$CLASSPATH" +
      s" $mainClass" +
      s" -version $version" +
      " 1>" + logdir +"/" + ApplicationConstants.STDOUT +
      " 2>" + logdir +"/" + ApplicationConstants.STDERR

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
    //TODO: if jarDir didn't file will be created instead of dir    
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
        LOG.error(s"Invalid value $memory defaulting to 1G")
        1024
    }
    capability.setMemory(containerMemory)
    capability.setVirtualCores(getEnv(YARNAPPMASTER_VCORES).toInt)
    capability
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
    LOG.info("Starting AM")
    //uploadAMResourcesToHDFS()
    yarnClient.init(yarnConf)
    yarnClient.start()
    val appContext = yarnClient.createApplication.getApplicationSubmissionContext
    appContext.setApplicationName(getEnv(YARNAPPMASTER_NAME))

    appContext.setAMContainerSpec(YarnContainerUtil.getContainerContext(yarnConf, version, getCommand))
    appContext.setResource(getAMCapability)
    appContext.setQueue(getEnv(YARNAPPMASTER_QUEUE))
    
    yarnClient.submitApplication(appContext)
    monitorAM(appContext)
  }

  deploy()
}

object Client extends App with ArgumentsParser {
  
  case class ClusterResources(totalFreeMemory: Long, totalContainers: Int, nodeManagersFreeMemory: Map[String, Long])

  override val options: Array[(String, CLIOption[Any])] = Array(
    "jars" -> CLIOption[String]("<AppMaster jar directory>", required = false),
    "version" -> CLIOption[String]("<gearpump version, we allow multiple gearpump version to co-exist on yarn>", required = true),
    "main" -> CLIOption[String]("<AppMaster main class>", required = false),
    "monitor" -> CLIOption[Boolean]("<monitor AppMaster state>", required = false, defaultValue = Some(false))
  )
  
  new Client(new AppConfig(parse(args), ConfigFactory.load), new YarnConfiguration, YarnClient.createYarnClient)
}

