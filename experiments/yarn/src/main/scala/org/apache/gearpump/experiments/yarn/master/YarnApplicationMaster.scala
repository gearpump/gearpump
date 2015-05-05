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
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records.{Container, ContainerId, FinalApplicationStatus}
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.slf4j.Logger

object AmActorProtocol {

  sealed trait RMTerminalState {
    val containerStats: ContainerStats
  }
  case class RMError(throwable: Throwable, containerStats: ContainerStats) extends RMTerminalState
  case class RMShutdownRequest(containerStats: ContainerStats) extends RMTerminalState
  case class RMAllRequestedContainersCompleted(containerStats: ContainerStats) extends RMTerminalState

  //protocol
  case class AMStatusMessage(appStatus: FinalApplicationStatus, appMessage: String, appTrackingUrl: String)
  case class AdditionalContainersRequest(count: Int)
  case class LaunchWorkerContainers(containers: List[Container])
  case class LaunchServiceContainer(containers: List[Container])
  case class ContainersRequest(memory: Int, vCores: Int)
  case class ContainersAllocated(containers: List[Container])
  case class ContainerStarted(containerId: ContainerId)
  case class ContainerStats(allocated: Int, completed: Int, failed: Int)
  case class RegisterAMMessage(appHostName: String, appHostPort: Int, appTrackingUrl: String)
  case class RegisterAppMasterResponse(response: RegisterApplicationMasterResponse)
  case object RMConnected
  case class RMConnectionFailed(throwable: Throwable)
}


class YarnApplicationMaster(appConfig: AppConfig, yarnConf: YarnConfiguration,
                            propsRMClient: Props,
                            createNMClient: (YarnConfiguration, ActorRef) => NMClientAsync,
                            containerLaunchContextFactory: (String) => org.apache.hadoop.yarn.api.records.ContainerLaunchContext) extends Actor {

  import AmActorProtocol._

  val LOG: Logger = LogUtil.getLogger(getClass)
  val nodeManagerClient: NMClientAsync = createNMClient(yarnConf, self)
  val resourceManagerClient = context.actorOf(propsRMClient, "resourceManagerClient")

  var masterContainers = Map.empty[ContainerId, (String, Int)]
  val host = InetAddress.getLocalHost.getHostName
  val servicesPort = appConfig.getEnv(SERVICES_PORT).toInt
  val trackingURL = "http://" + host + ":" + servicesPort
  var masterAddr: Option[HostPort] = None
  var masterContainersStarted = 0
  var workerContainersStarted = 0
  var workerContainersRequested = 0
  val version = appConfig.getEnv("version")

  var servicesActor: Option[ActorRef] = None

  override def receive: Receive = {

    case RMConnected =>
      LOG.info("received RMConnected")
      val port = appConfig.getEnv(YARNAPPMASTER_PORT).toInt
      val target = host + ":" + port
      val addr = NetUtils.createSocketAddr(target)
      resourceManagerClient ! RegisterAMMessage(addr.getHostName, port, trackingURL)

    case RMConnectionFailed(throwable) =>
      LOG.info("Failed to connect to Resource Manager", throwable.getMessage)

    case ContainersAllocated(containers) =>
      LOG.info("Received LaunchContainers")
      if (needMoreMasterContainersState) {
        LOG.info(s"Launching more masters : ${containers.size}")
        setMasterAddrIfNeeded(containers)
        launchMasterContainers(containers)
      } else if (needMoreWorkerContainersState) {
        LOG.info(s"Launching more workers : ${containers.size}")
        workerContainersRequested += containers.size
        launchWorkerContainers(containers)
      } else {
        LOG.info("No more needed")
      }

    case ContainerStarted(containerId) =>
      LOG.info(s"Started container : $containerId")
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

    case AdditionalContainersRequest(count) =>
      LOG.info("AM: Received AdditionalContainersRequest($count)")
      requestMoreContainers(count)

    case containerRequest: ContainersRequest =>
      LOG.info("AM: Received ContainerRequestMessage")
      resourceManagerClient ! containerRequest

    case RegisterAppMasterResponse(response) =>
      LOG.info("Received RegisterAppMasterResponse")
      requestMasterContainers(response)

    case state: RMTerminalState =>
      LOG.info("Got Terminal State")
      cleanUp(state)

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

  private def needMoreMasterContainersState: Boolean = {
    masterContainersStarted < appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt
  }

  private def needMoreWorkerContainersState: Boolean = {
    workerContainersStarted < appConfig.getEnv(WORKER_CONTAINERS).toInt
  }

  private def requestWorkerContainersIfNeeded(): Unit = {
    if (masterContainersStarted == appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt) {
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
      masterContainers += container.getId ->(container.getNodeId.getHost, port)
    })
  }

  private def launchWorkerContainers(containers: List[Container]) {
    containers.foreach(container => {
      val workerHost = container.getNodeId.getHost
      val workerCommand = WorkerContainerCommand(appConfig, masterAddr.get, workerHost).getCommand
      launchCommand(container, workerCommand)
    })
  }

  private def launchCommand(container: Container, command: String) {
    LOG.info(s"Launching containter: containerId :  ${container.getId}, host ip : ${container.getNodeId.getHost}")
    LOG.info("Launching command : " + command)
    val containerContext = containerLaunchContextFactory(command)
    LOG.info(s"NodeManagerClient : $nodeManagerClient, container : $container, containerContext $containerContext")
    nodeManagerClient.startContainerAsync(container, containerContext)
  }

  private def requestWorkerContainers(): Unit = {
    (1 to appConfig.getEnv(WORKER_CONTAINERS).toInt).foreach(requestId => {
      resourceManagerClient ! ContainersRequest(appConfig.getEnv(WORKER_MEMORY).toInt, appConfig.getEnv(WORKER_VCORES).toInt)
    })
  }

  private def requestMoreContainers(count: Int): Unit = {
    (1 to count).foreach((i) => resourceManagerClient ! ContainersRequest(appConfig.getEnv(WORKER_MEMORY).toInt, appConfig.getEnv(WORKER_VCORES).toInt))
  }

  private def requestMasterContainers(registrationResponse: RegisterApplicationMasterResponse) = {
    //TODO remove that or do something more with that
    val previousContainersCount = registrationResponse.getContainersFromPreviousAttempts.size

    LOG.info(s"Previous container count : $previousContainersCount")
    if (previousContainersCount > 0) {
      LOG.warn("Previous container count > 0, can't do anything with it")
    }

    LOG.info(s"GEARPUMPMASTER_CONTAINERS: ${appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt}")
    (1 to appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt).foreach(requestId => {
      resourceManagerClient ! ContainersRequest(appConfig.getEnv(GEARPUMPMASTER_MEMORY).toInt, appConfig.getEnv(GEARPUMPMASTER_VCORES).toInt)
    })

  }

  private def cleanUp(state: RMTerminalState): Unit = {
    LOG.info("Application completed. Stopping running containers")
    nodeManagerClient.stop()

    state match {
      case RMError(throwable, stats) =>
        LOG.info("Failed", throwable.getMessage)
        val message = s"Failed. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
        resourceManagerClient ! AMStatusMessage(FinalApplicationStatus.FAILED, message, null)
        
      case RMShutdownRequest(stats) =>
        if (stats.failed == 0 && stats.completed == appConfig.getEnv(WORKER_CONTAINERS).toInt) {
          val message = s"ShutdownRequest. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
          resourceManagerClient ! AMStatusMessage(FinalApplicationStatus.KILLED, message, null)
        } else {
          val message = s"ShutdownRequest. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
          resourceManagerClient ! AMStatusMessage(FinalApplicationStatus.FAILED, message, null)
        }

      case RMAllRequestedContainersCompleted(stats) =>
        val message = s"Diagnostics. total=${appConfig.getEnv(WORKER_CONTAINERS).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
        resourceManagerClient ! AMStatusMessage(FinalApplicationStatus.SUCCEEDED, message, null)
    }

    resourceManagerClient ! PoisonPill
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
  private[this]   def loadYarnConfUsingHadoopConfEnv: YarnConfiguration = {
    val hadoopConf = new Configuration(true)
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
      val yarnConfiguration = loadYarnConfUsingHadoopConfEnv
      LOG.info("HADOOP_CONF_DIR: " + System.getenv("HADOOP_CONF_DIR"))
      LOG.info("Yarn config (yarn.resourcemanager.hostname): " + yarnConfiguration.get("yarn.resourcemanager.hostname"))
      val amActorProps = Props(
        new YarnApplicationMaster(appConfig,
          yarnConfiguration,
          Props(classOf[ResourceManagerClient], yarnConfiguration, appConfig),
          (yarnConf, am) => {
            val nmClient = new NMClientAsyncImpl(new NodeManagerCallbackHandler(am))
            nmClient.init(yarnConf)
            nmClient.start()
            nmClient
          },
          ContainerLaunchContext(yarnConfiguration, appConfig)))
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

