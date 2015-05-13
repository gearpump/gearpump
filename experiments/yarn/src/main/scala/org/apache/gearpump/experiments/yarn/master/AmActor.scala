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

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props, actorRef2Scala}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.experiments.yarn.CmdLineVars.{APPMASTER_IP, APPMASTER_PORT}
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.gearpump.experiments.yarn._
import org.apache.gearpump.experiments.yarn.master.AmActor.{RMCallbackHandlerActorProps, RMClientActorProps}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records.{Container, ContainerId, FinalApplicationStatus}
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.slf4j.Logger

import scala.util.{Failure, Success, Try}

object AmActor {
  case class RMCallbackHandlerActorProps(props: Props)
  case class RMClientActorProps(props: Props)

  def createAMRMClient(intervalMs: Int, callbackHandler: AMRMClientAsync.CallbackHandler): AMRMClientAsync[ContainerRequest] = {
    AMRMClientAsync.createAMRMClientAsync(intervalMs, callbackHandler)
  }

  def createNodeManagerCallbackHandler(am: ActorRef): NodeManagerCallbackHandler = new NodeManagerCallbackHandler(am)

  def createNMClient(containerListener: NodeManagerCallbackHandler, yarnConf: YarnConfiguration): NMClientAsync = {
    val nmClient = new NMClientAsyncImpl(containerListener)
    nmClient.init(yarnConf)
    nmClient.start()
    nmClient
  }

  def getRMCallbackHandlerActorProps(appConfig: AppConfig) : RMCallbackHandlerActorProps =  RMCallbackHandlerActorProps(Props(classOf[RMCallbackHandlerActor], appConfig))
  def getRMClientActorProps(yarnConfiguration: YarnConfiguration) : RMClientActorProps =  RMClientActorProps(Props(classOf[ResourceManagerClient], yarnConfiguration, createAMRMClient _))
}

object AmStates {
  sealed trait Reason
  case class Failed(throwable: Throwable) extends Reason
  case object ShutdownRequest extends Reason
  case object AllRequestedContainersCompleted extends Reason
}

object AmActorProtocol {
  import AmStates._

  sealed trait YarnApplicationMasterData
  case class AMRMClientAsyncStartup(status:Try[Boolean])
  case class LaunchContainers(containers: List[Container]) extends YarnApplicationMasterData
  case class LaunchWorkerContainers(containers: List[Container])
  case class LaunchServiceContainer(containers: List[Container])
  case class ContainerRequestMessage(memory: Int, vCores: Int)
  case class RMHandlerDone(reason: Reason, rMHandlerContainerStats: RMHandlerContainerStats)
  case class RMHandlerContainerStats(allocated: Int, completed: Int, failed: Int)
  case class RegisterAMMessage(appHostName: String, appHostPort: Int, appTrackingUrl: String)
  case class RegisterAppMasterResponse(response: RegisterApplicationMasterResponse)
  case class AMStatusMessage(appStatus: FinalApplicationStatus, appMessage: String, appTrackingUrl: String)
  case class ContainerStarted(containerId: ContainerId)
}


class AmActor(appConfig: AppConfig, yarnConf: YarnConfiguration,
              rmCallbackHandlerActorProps: RMCallbackHandlerActorProps,
              rmClientActorProps: RMClientActorProps,
              createNMClient: (NodeManagerCallbackHandler, YarnConfiguration) => NMClientAsync,
              createNodeManagerCallbackHandler: (ActorRef) => NodeManagerCallbackHandler,
              containerLaunchContextFactory: ContainerLaunchContextFactory) extends Actor {
  import AmActorProtocol._
  import AmStates._

  val LOG: Logger = LogUtil.getLogger(getClass)
  val nodeManagerCallbackHandler = createNodeManagerCallbackHandler(self)
  val nodeManagerClient: NMClientAsync = createNMClient(nodeManagerCallbackHandler, yarnConf)
  val rmCallbackHandlerActor = context.actorOf(rmCallbackHandlerActorProps.props, "rmCallbackHandler")
  val rmClientActor = context.actorOf(rmClientActorProps.props, "rmClient")

  var masterContainers = Map.empty[ContainerId, (String, Int)]
  val host = InetAddress.getLocalHost.getHostName
  val servicesPort = appConfig.getEnv(SERVICES_PORT).toInt
  val trackingURL = "http://"+host+":"+servicesPort
  var masterAddr: Option[HostPort] = None
  var masterContainersStarted = 0
  var workerContainersStarted = 0
  var workerContainersRequested = 0
  val version = appConfig.getEnv("version")

  var servicesActor: Option[ActorRef] = None

  override def receive: Receive = {

    case AMRMClientAsyncStartup(status) =>
      status match {
        case Success(true) =>
          LOG.error("received AMRMClientAsyncStartup")
          val port = appConfig.getEnv(YARNAPPMASTER_PORT).toInt
          val target = host + ":" + port
          val addr = NetUtils.createSocketAddr(target)
          rmClientActor ! RegisterAMMessage(addr.getHostName, port, trackingURL)
        case Success(false) =>
          LOG.error("Failed to start AMRMClientAsync")
        case Failure(ex) =>
          LOG.error("Failed to start AMRMClientAsync", ex)
        case _ =>
          LOG.error("Unknown status for AMRMClientAysncStartup")
      }

    case containerStarted: ContainerStarted =>
      LOG.info(s"Started container : ${containerStarted.containerId}")
      if (needMoreMasterContainersState) {
        masterContainersStarted += 1
        LOG.info(s"Currently master containers started : $masterContainersStarted/${appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt}")
          requestWorkerContainersIfNeeded()
      } else {
        workerContainersStarted += 1
        LOG.info(s"Currently worker containers started : $workerContainersStarted/${appConfig.getEnv(WORKER_CONTAINERS).toInt}")
        servicesActor match {
          case None if workerContainersStarted == workerContainersRequested =>
            val masters = masterContainers.map(pair => {
              val (_, (host, port)) = pair
              host + ":" + port
            }).toArray
            servicesActor = Some(context.actorOf(Props(classOf[ServicesLauncherActor], masters, host, servicesPort)))
          case _ =>
        }
      }
      
    case containerRequest: ContainerRequestMessage =>
      LOG.info("AM: Received ContainerRequestMessage")
      rmClientActor ! containerRequest
    
    case rmCallbackHandler: ResourceManagerCallbackHandler =>
      LOG.info("Received RMCallbackHandler")
      rmClientActor forward rmCallbackHandler

    case amResponse: RegisterAppMasterResponse =>
      LOG.info("Received RegisterAppMasterResponse")
      requestMasterContainers(amResponse.response)

    case containers: LaunchContainers =>
      LOG.info("Received LaunchContainers")
      if(needMoreMasterContainersState) {
        LOG.info(s"Launching more masters : ${containers.containers.size}")
        setMasterAddrIfNeeded(containers.containers)
        launchMasterContainers(containers.containers)        
      } else if(needMoreWorkerContainersState){ 
        LOG.info(s"Launching more workers : ${containers.containers.size}")
        workerContainersRequested += containers.containers.size
        launchWorkerContainers(containers.containers)
      } else {
        LOG.info("No more needed")
      }
      
    case done: RMHandlerDone =>
      LOG.info("Got RMHandlerDone")
      cleanUp(done)

    case unknown =>
      LOG.info(s"Unknown message ${unknown.getClass.getName}")

  }

  private def setMasterAddrIfNeeded(containers: List[Container]) {
    masterAddr match {
      case None =>
        masterAddr = Some(HostPort(containers.head.getNodeId.getHost, appConfig.getEnv(GEARPUMPMASTER_PORT).toInt))
      case _ =>
    }

  }

  private def needMoreMasterContainersState:Boolean = {
    masterContainersStarted < appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt
  }

  private def needMoreWorkerContainersState:Boolean = {
    workerContainersStarted < appConfig.getEnv(WORKER_CONTAINERS).toInt
  }

  private def requestWorkerContainersIfNeeded(): Unit = {
    if(masterContainersStarted == appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt) {
      LOG.info("Requesting worker containers")
      requestWorkerContainers()
    }
  }

  private def launchMasterContainers(containers: List[Container]) {
    LOG.info(s"Containers size ${containers.size}")
    containers.foreach(container => {
      val port = appConfig.getEnv(GEARPUMPMASTER_PORT).toInt
      val masterCommand = MasterContainerCommand(appConfig, masterAddr.get)
      launchCommand(container, masterCommand.getCommand)
      masterContainers += container.getId -> (container.getNodeId.getHost, port)
    })
  }

  private def launchWorkerContainers(containers: List[Container]) {
    containers.foreach(container => {
      val workerHost = container.getNodeId.getHost
      val workerCommand = WorkerContainerCommand(appConfig, masterAddr.get, workerHost).getCommand
      launchCommand(container, workerCommand)
    })
  }

  private def launchCommand(container: Container, command:String) {
      LOG.info(s"Launching containter: containerId :  ${container.getId}, host ip : ${container.getNodeId.getHost}")
      LOG.info("Launching command : " + command)
      val containerContext = containerLaunchContextFactory.newInstance(command)
      LOG.info(s"NodeManagerClient : $nodeManagerClient, container : $container, containerContext $containerContext")
      nodeManagerClient.startContainerAsync(container, containerContext)
  }

  private def requestWorkerContainers(): Unit = {
    (1 to appConfig.getEnv(WORKER_CONTAINERS).toInt).foreach(requestId => {
      rmClientActor ! ContainerRequestMessage(appConfig.getEnv(WORKER_MEMORY).toInt, appConfig.getEnv(WORKER_VCORES).toInt)
    })
  }

  private def requestMasterContainers(registrationResponse: RegisterApplicationMasterResponse) = {
    //TODO remove that or do something more with that
    val previousContainersCount = registrationResponse.getContainersFromPreviousAttempts.size
    
    LOG.info(s"Previous container count : $previousContainersCount")
    if(previousContainersCount > 0) {
      LOG.warn("Previous container count > 0, can't do anything with it")
    }

    LOG.info(s"GEARPUMPMASTER_CONTAINERS: ${appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt}")
    (1 to appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt).foreach(requestId => {
      rmClientActor ! ContainerRequestMessage(appConfig.getEnv(GEARPUMPMASTER_MEMORY).toInt, appConfig.getEnv(GEARPUMPMASTER_VCORES).toInt)
    })

  }

  private def cleanUp(done: RMHandlerDone): Boolean = {
    LOG.info("Application completed. Stopping running containers")
    nodeManagerClient.stop()
    var success = true

    val stats = done.rMHandlerContainerStats
    done.reason match {
      case failed: Failed =>
        val message = s"Failed. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
        rmClientActor ! AMStatusMessage(FinalApplicationStatus.FAILED, message, null)
        success = false
      case ShutdownRequest =>
        if (stats.failed == 0 && stats.completed == appConfig.getEnv(WORKER_CONTAINERS).toInt) {
          val message = s"ShutdownRequest. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
          rmClientActor ! AMStatusMessage(FinalApplicationStatus.KILLED, message, null)
          success = false
        } else {
          val message = s"ShutdownRequest. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
          rmClientActor ! AMStatusMessage(FinalApplicationStatus.FAILED, message, null)
          success = false
        }
       case AllRequestedContainersCompleted =>
        val message = s"Diagnostics. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
        rmClientActor ! AMStatusMessage(FinalApplicationStatus.SUCCEEDED, message, null)
        success = true
    }

    rmClientActor ! PoisonPill
    success
    }

}

class RMCallbackHandlerActor(appConfig: AppConfig) extends Actor {
  val LOG: Logger = LogUtil.getLogger(getClass)
  val rmCallbackHandler = new ResourceManagerCallbackHandler(appConfig, context.parent)

  override def preStart(): Unit = {
    LOG.info("Sending RMCallbackHandler to parent (YarnAM)")
    context.parent ! rmCallbackHandler
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
    APPMASTER_PORT -> CLIOption[String]("<Gearpump master port>", required = false),
    "version" -> CLIOption[String]("<Gearpump version>", required = true)
  )

  /**
   * For yet unknown reason this is needed for my local pseudo distributed cluster.   
   * 
   */
  def getForcedDefaultYarnConf:YarnConfiguration = {
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
      val config = ConfigFactory.parseResourcesAnySyntax(YARN_CONFIG)
      implicit val system = ActorSystem("GearPumpAM", config)
      val appConfig = new AppConfig(parse(args), config)
      val yarnConfiguration = getForcedDefaultYarnConf
      LOG.info("HADOOP_CONF_DIR: " + System.getenv("HADOOP_CONF_DIR"))
      LOG.info("Yarn config (yarn.resourcemanager.hostname): " + yarnConfiguration.get("yarn.resourcemanager.hostname"))
      LOG.info("Creating AMActor v1.5")
      import AmActor._
      val amActorProps = Props(
        new AmActor(appConfig,
                    yarnConfiguration,
                    getRMCallbackHandlerActorProps(appConfig),
                    getRMClientActorProps(yarnConfiguration),
                    createNMClient _,
                    createNodeManagerCallbackHandler _,
                    DefaultContainerLaunchContextFactory(yarnConfiguration, appConfig)))
      system.actorOf(amActorProps, "GearPumpAMActor")
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

