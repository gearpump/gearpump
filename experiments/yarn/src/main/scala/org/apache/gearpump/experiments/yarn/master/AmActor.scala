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

package org.apache.gearpump.experiments.yarn.master

import java.net.InetAddress
import java.util.concurrent.TimeUnit
import org.apache.gearpump.cluster.main.ArgumentsParser
import org.apache.gearpump.cluster.main.CLIOption
import org.apache.gearpump.experiments.yarn.Actions.AMStatusMessage
import org.apache.gearpump.experiments.yarn.Actions.AllRequestedContainersCompleted
import org.apache.gearpump.experiments.yarn.Actions.ContainerInfo
import org.apache.gearpump.experiments.yarn.Actions.ContainerRequestMessage
import org.apache.gearpump.experiments.yarn.Actions.Failed
import org.apache.gearpump.experiments.yarn.Actions.LaunchContainers
import org.apache.gearpump.experiments.yarn.Actions.RMHandlerDone
import org.apache.gearpump.experiments.yarn.Actions.RegisterAMMessage
import org.apache.gearpump.experiments.yarn.Actions.ShutdownRequest
import org.apache.gearpump.experiments.yarn.AppConfig
import org.apache.gearpump.experiments.yarn.CmdLineVars.APPMASTER_IP
import org.apache.gearpump.experiments.yarn.CmdLineVars.APPMASTER_PORT
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.gearpump.experiments.yarn.NodeManagerCallbackHandler
import org.apache.gearpump.experiments.yarn.ResourceManagerClientActor
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.slf4j.Logger
import com.typesafe.config.ConfigFactory
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.util.Timeout
import akka.actor.FSM
import org.apache.gearpump.experiments.yarn.Actions._
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.transport.HostPort

import org.apache.gearpump.util.Constants.{GEARPUMP_CLUSTER_MASTERS,GEARPUMP_LOG_DAEMON_DIR,GEARPUMP_LOG_APPLICATION_DIR }


/**
 * Yarn ApplicationMaster.
 */
class AmActor(appConfig: AppConfig, yarnConf: YarnConfiguration) extends Actor {
  val LOG: Logger = LogUtil.getLogger(getClass)
  val nodeManagerCallbackHandler = createNodeManagerCallbackHandler
  val nodeManagerClient: NMClientAsync = createNMClient(nodeManagerCallbackHandler)
  val rmCallbackHandler = context.actorOf(Props(classOf[RMCallbackHandlerActor], appConfig, self), "rmCallbackHandler")
  val amRMClient = context.actorOf(Props(classOf[ResourceManagerClientActor], yarnConf, self), "amRMClient")
  val containersStatus = collection.mutable.Map[Long, ContainerInfo]()
  
  var masterAddr:HostPort = _ 
  var masterContainersStarted = 0
  var workerContainersStarted = 0
  var workerContainersRequested = 0
  
  override def receive: Receive = {
    case containerStarted: ContainerStarted =>
      LOG.info(s"Started container : ${containerStarted.containerId}") 
      if(needMoreMasterContainersState) {
        masterContainersStarted += 1        
        LOG.info(s"Currently master containers started : $masterContainersStarted/${appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt}")
        requestWorkerContainersIfNeeded
      } else {
        workerContainersStarted += 1
        LOG.info(s"Currently worker containers started : $workerContainersStarted/${appConfig.getEnv(WORKER_CONTAINERS).toInt}")
      }
      
    case containerRequest: ContainerRequestMessage =>
      LOG.info("AM: Received ContainerRequestMessage")
      amRMClient ! containerRequest
    
    case rmCallbackHandler: ResourceManagerCallbackHandler =>
      LOG.info("Received RMCallbackHandler")
      amRMClient forward rmCallbackHandler
      val host = InetAddress.getLocalHost().getHostName();
      val port = appConfig.getEnv(YARNAPPMASTER_PORT).toInt
      val target = host + ":" + port
      val addr = NetUtils.createSocketAddr(target);
      amRMClient ! RegisterAMMessage(addr.getHostName, port, "")
    
    case amResponse: RegisterApplicationMasterResponse =>
      LOG.info("Received RegisterApplicationMasterResponse")
      requestMasterContainers(amResponse)

    case containers: LaunchContainers =>
      LOG.info("Received LaunchContainers")
      if(needMoreMasterContainersState) {
        LOG.info(s"Launching more masters : ${containers.containers.size}")
        setMasterAddrIfNeeded(containers.containers)
        launchMasterContainers(containers.containers)        
      } else if(needMoreWorkerContainersState){ 
        LOG.info(s"Launching more workers : ${containers.containers.size}")
        workerContainersRequested += containers.containers.size
        launchWorkerContainers(containers.containers, masterAddr)
      } else {
        LOG.info("No more needed")
      }
      
    case done: RMHandlerDone =>
      LOG.info("Got RMHandlerDone")
      cleanUp(done)
  
  }

  private[this] def setMasterAddrIfNeeded(containers: List[Container]) {
    if(masterAddr == null)
    masterAddr = HostPort(containers.head.getNodeId.getHost, appConfig.getEnv(GEARPUMPMASTER_PORT).toInt) 
  }

  private[this] def needMoreMasterContainersState:Boolean = {
    masterContainersStarted < appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt
  }

  private[this] def needMoreWorkerContainersState:Boolean = {
    workerContainersStarted < appConfig.getEnv(WORKER_CONTAINERS).toInt
  }

  private[this] def requestWorkerContainersIfNeeded { 
    if(masterContainersStarted == appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt) {
      LOG.info("Requesting worker containers")
      requestWorkerContainers
    }
  }

  private[this] def launchMasterContainers(containers: List[Container]) {
    containers.foreach(container => {
      launchCommand(container, getMasterCommand(container.getNodeId.getHost, appConfig.getEnv(GEARPUMPMASTER_PORT).toInt))
    })
  }

  private[this] def launchWorkerContainers(containers: List[Container], masterAddr: HostPort) {
    containers.foreach(container => {
      val masterHost = masterAddr.host
      val masterPort = masterAddr.port
      val workerHost = container.getNodeId.getHost
      launchCommand(container, getWorkerCommand(masterHost, masterPort, workerHost))
    })
  }

  private[this] def launchCommand(container: Container, command:String) {
      LOG.info(s"Launching containter: containerId :  ${container.getId}, host ip : ${container.getNodeId.getHost}")
      LOG.info("Launching command : " + command)
      context.actorOf(Props(classOf[ContainerLauncherActor], container, nodeManagerClient, yarnConf, command))
  }

  private[this] def getMasterCommand(masterHost: String, masterPort: Int): String = {
    val masterArguments = s"-ip $masterHost -port $masterPort"

    val properties = Array(
      s"-D${GEARPUMP_CLUSTER_MASTERS}.0=${masterHost}:${masterPort}",
      s"-D${GEARPUMP_LOG_DAEMON_DIR}=${ApplicationConstants.LOG_DIR_EXPANSION_VAR}",
      s"-D${GEARPUMP_LOG_APPLICATION_DIR}=${ApplicationConstants.LOG_DIR_EXPANSION_VAR}")

    getCommand(GEARPUMPMASTER_COMMAND, properties, GEARPUMPMASTER_MAIN,
      masterArguments, GEARPUMPMASTER_LOG)
  }

  private[this] def getWorkerCommand(masterHost: String, masterPort: Int, workerHost: String): String = {

    val arguments = s"-ip $workerHost"
    val properties = Array(
      s"-D${GEARPUMP_CLUSTER_MASTERS}.0=${masterHost}:${masterPort}",
      s"-D${GEARPUMP_LOG_DAEMON_DIR}=${ApplicationConstants.LOG_DIR_EXPANSION_VAR}",
      s"-D${GEARPUMP_LOG_APPLICATION_DIR}=${ApplicationConstants.LOG_DIR_EXPANSION_VAR}")
    getCommand(WORKER_COMMAND, properties,  WORKER_MAIN, arguments, WORKER_LOG)
  }
  
  private[this] def getCommand(java: String, properties: Array[String], mainProp: String, cliOpts: String, lognameProp: String):String = {
    val exe = appConfig.getEnv(java)
    val main = appConfig.getEnv(mainProp)
    val logname = appConfig.getEnv(lognameProp)
    s"$exe ${properties.mkString(" ")}  $main $cliOpts 2>&1 | /usr/bin/tee -a ${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/$logname"
  }
  

  private[this] def createNMClient(containerListener: NodeManagerCallbackHandler): NMClientAsync = {
    LOG.info("Creating NMClientAsync")
    val nmClient = new NMClientAsyncImpl(containerListener)
    LOG.info("Yarn config : " + yarnConf.get("yarn.resourcemanager.hostname"))
    nmClient.init(yarnConf)
    nmClient.start()
    
    nmClient
  }

  private[this] def createNodeManagerCallbackHandler: NodeManagerCallbackHandler = {
    LOG.info("Creating NMCallbackHandler")
    new NodeManagerCallbackHandler(self)
  }


  private[this] def requestWorkerContainers {
    (1 to appConfig.getEnv(WORKER_CONTAINERS).toInt).foreach(requestId => {
      amRMClient ! ContainerRequestMessage(appConfig.getEnv(WORKER_MEMORY).toInt, appConfig.getEnv(WORKER_VCORES).toInt)
    })

  }

  private[this] def requestMasterContainers(registrationResponse: RegisterApplicationMasterResponse) {
    val previousContainersCount = registrationResponse.getContainersFromPreviousAttempts.size
    
    LOG.info(s"Previous container count : $previousContainersCount")
    if(previousContainersCount > 0) {
      LOG.warn("Previous container count > 0, can't do anything with it")
    }
    
    (1 to appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt).foreach(requestId => {
      amRMClient ! ContainerRequestMessage(appConfig.getEnv(GEARPUMPMASTER_MEMORY).toInt, appConfig.getEnv(GEARPUMPMASTER_VCORES).toInt)
    })

  }

  private[this] def cleanUp(done: RMHandlerDone): Boolean = {
    LOG.info("Application completed. Stopping running containers")
    nodeManagerClient.stop()
    var success = true

    val stats = done.rMHandlerContainerStats
    done.reason match {
      case failed: Failed =>
        val message = s"Failed. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
        amRMClient ! AMStatusMessage(FinalApplicationStatus.FAILED, message, null)
        success = false
      case ShutdownRequest =>
        if (stats.failed == 0 && stats.completed == appConfig.getEnv(WORKER_CONTAINERS).toInt) {
          val message = s"ShutdownRequest. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
          amRMClient ! AMStatusMessage(FinalApplicationStatus.KILLED, message, null)
          success = false
        } else {
          val message = s"ShutdownRequest. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
          amRMClient ! AMStatusMessage(FinalApplicationStatus.FAILED, message, null)
          success = false
        }
       case AllRequestedContainersCompleted =>
        val message = s"Diagnostics. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
        amRMClient ! AMStatusMessage(FinalApplicationStatus.SUCCEEDED, message, null)
        success = true
    }

    amRMClient ! PoisonPill
    success
    }
}
 
class RMCallbackHandlerActor(appConfig: AppConfig, yarnAM: ActorRef) extends Actor {
  val LOG: Logger = LogUtil.getLogger(getClass)
  val rmCallbackHandler = new ResourceManagerCallbackHandler(appConfig, yarnAM)

  override def preStart(): Unit = {
    LOG.info("Sending RMCallbackHandler to YarnAM")
    yarnAM ! rmCallbackHandler
  }

  override def receive: Receive = {
    case _ =>
      LOG.error(s"Unknown message received")
  }

}


object YarnApplicationMaster extends App with ArgumentsParser {
  val LOG: Logger = LogUtil.getLogger(getClass)
  val TIME_INTERVAL = 1000

  override val options: Array[(String, CLIOption[Any])] = Array(
    APPMASTER_IP -> CLIOption[String]("<Gearpump master ip>", required = false),
    APPMASTER_PORT -> CLIOption[String]("<Gearpump master port>", required = false)
  )

  /**
   * For yet unknown reason this is needed for my local pseudo distributed cluster.   
   * 
   */
  def getForcedDefaultYarnConf:Configuration = {
      val hadoopConf  = new Configuration(true)
      val configDir = System.getenv("HADOOP_CONF_DIR")
      Configuration.addDefaultResource(configDir + "/core-site.xml")
      Configuration.addDefaultResource(configDir + "/hdfs-site")
      Configuration.addDefaultResource(configDir + "/yarn-site.xml")
      new YarnConfiguration(hadoopConf)
  }
  
  def apply(args: Array[String]) = {
    try {
      implicit val timeout = Timeout(5, TimeUnit.SECONDS)
      val config = ConfigFactory.load
      implicit val system = ActorSystem("GearPumpAM", config)
      val appConfig = new AppConfig(parse(args), config)
      val yarnConfiguration = getForcedDefaultYarnConf
      LOG.info("HADOOP_CONF_DIR: " + System.getenv("HADOOP_CONF_DIR"))
      LOG.info("Yarn config (yarn.resourcemanager.hostname): " + yarnConfiguration.get("yarn.resourcemanager.hostname"))
      LOG.info("Creating AMActor v1.5")
      system.actorOf(Props(classOf[AmActor], appConfig, yarnConfiguration), "GearPumpAMActor")
      system.awaitTermination()
      LOG.info("Shutting down")
      system.shutdown()
    } catch {
      case throwable: Throwable =>
        LOG.error("Caught exception", throwable)
        throwable.printStackTrace()
    }

  }

  apply(args)

}