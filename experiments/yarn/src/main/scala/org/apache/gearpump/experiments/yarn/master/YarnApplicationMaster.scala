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

import akka.actor._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.experiments.yarn.CmdLineVars.{APPMASTER_IP, APPMASTER_PORT}
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.gearpump.experiments.yarn.master.AmActorProtocol.ContainerInfo
import org.apache.gearpump.experiments.yarn.{ContainerLaunchContext, _}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.slf4j.Logger

object AmActorProtocol {

  sealed trait RMTerminalState
  case class RMError(throwable: Throwable) extends RMTerminalState
  case object RMShutdownRequest extends RMTerminalState
  case class AMShutdownRequest(stats:String) extends RMTerminalState

  sealed trait ContainerType
  case object MASTER extends ContainerType
  case object WORKER extends ContainerType
  case object SERVICE extends ContainerType

  //protocol
  case class AdditionalContainersRequest(count: Int)
  case class LaunchWorkerContainers(containers: List[Container])
  case class LaunchServiceContainer(containers: List[Container])
  case class ContainersRequest(containers: List[Resource])
  case class ContainersAllocated(containers: List[Container])
  case class ContainersCompleted(containers: List[ContainerStatus])
  case class ContainerInfo(host:String, port:Int, containerType :ContainerType, status: ContainerStatus)
  case class ContainerStarted(containerId: ContainerId)
  case class ContainerStats(allocated: Int, completed: Int, failed: Int, requested: Int)
  case class RegisterAMMessage(appHostName: String, appHostPort: Int, appTrackingUrl: String)
  case class RegisterAppMasterResponse(response: RegisterApplicationMasterResponse)
  case object RMConnected
  case class RMConnectionFailed(throwable: Throwable)

  implicit def containerStats(stats: ContainerStats): String = {
    s"allocated=${stats.allocated} completed=${stats.completed} failed=${stats.failed} requested=${stats.requested}"
  }
}

sealed trait State
case object ConnectingToResourceManager extends State
case object RegisteringAppMaster extends State
case object LaunchingMasters extends State
case object LaunchingWorkers extends State
case object RequestingServices extends State
case object AwaitingTermination extends State

class YarnApplicationMaster(appConfig: AppConfig, yarnConf: YarnConfiguration,
                            propsRMClient: Props,
                            createNMClient: (YarnConfiguration, ActorRef) => NMClientAsync,
                            containerLaunchContextFactory: (String) => org.apache.hadoop.yarn.api.records.ContainerLaunchContext,
                            initializedClusterStats: YarnClusterStats)
  extends FSM[State, YarnClusterStats] {

  import AmActorProtocol._

  private val LOG: Logger = LogUtil.getLogger(getClass)
  private val nodeManagerClient: NMClientAsync = createNMClient(yarnConf, self)
  private val servicesPort = appConfig.getEnv(SERVICES_PORT).toInt
  private[master] val resourceManagerClient = context.actorOf(propsRMClient, "resourceManagerClient")
  private[master] val host = InetAddress.getLocalHost.getHostName
  private[master] val trackingURL = "http://" + host + ":" + servicesPort
  private[master] var servicesActor: Option[ActorRef] = None

  startWith(ConnectingToResourceManager, initializedClusterStats)
  when(ConnectingToResourceManager) { connectionHandler }
  when(RegisteringAppMaster) { registerHandler }
  when(LaunchingMasters) { masterContainersHandler orElse containersCompletedHandler }
  when(LaunchingWorkers) { workerContainersHandler orElse containersCompletedHandler }
  when(AwaitingTermination) { containersCompletedHandler }
  whenUnhandled(terminalStateHandler orElse unknownHandler)
  initialize()

  def connectionHandler: StateFunction = {
    case Event(RMConnected, stats) =>
      LOG.info("received RMConnected")
      val port = appConfig.getEnv(YARNAPPMASTER_PORT).toInt
      val target = host + ":" + port
      val addr = NetUtils.createSocketAddr(target)
      resourceManagerClient ! RegisterAMMessage(addr.getHostName, port, trackingURL)
      goto(RegisteringAppMaster) using stats
    case Event(RMConnectionFailed(throwable), _) =>
      LOG.info("Failed to connect to Resource Manager", throwable.getMessage)
      self ! RMError(throwable)
      stay
  }

  def registerHandler: StateFunction = {
    case Event(RegisterAppMasterResponse(response), _) =>
      LOG.info("Received RegisterAppMasterResponse")
      requestMasterContainers(response)
      goto(LaunchingMasters)
  }

  /**
   * This handler is responsible for launching master processes inside containers allocated by Yarn.
   * In response to ContainersAllocated it launches new master processes.
   * In response to ContainerStarted it checks if configured number of masters has been reached. If so it moves
   * to the next state.
   * @return
   */
  private def masterContainersHandler : StateFunction = {
    case Event(ContainersAllocated(containers), yarnClusterStats: YarnClusterStats) =>
      val containersToLaunch = containers.filter(filterAllocatedAndWarn(yarnClusterStats)_)
      stay using containersToLaunch.foldLeft(yarnClusterStats)(launchMasterContainer)

    case Event(ContainerStarted(containerId), yarnClusterStats: YarnClusterStats) =>
      LOG.info(s"Started master container : $containerId")
      val newYarnClusterStats = updateContainer(containerId, yarnClusterStats)
      if(needMoreMasters(newYarnClusterStats)) {
        stay using newYarnClusterStats
      } else {
        requestWorkerContainers()
        goto(LaunchingWorkers) using newYarnClusterStats
      }
  }

  private def workerContainersHandler: StateFunction  = {
    case Event(ContainersAllocated(containers), yarnClusterStats: YarnClusterStats) =>
      val masterAddr = HostPort(yarnClusterStats.getRunningMasterContainersAddrs.head)
      val containersToLaunch = containers.filter(filterAllocatedAndWarn(yarnClusterStats)_)
      stay using containersToLaunch.foldLeft(yarnClusterStats)(launchWorkerContainer(masterAddr)_)

    case Event(ContainerStarted(containerId), yarnClusterStats: YarnClusterStats) =>
      LOG.info(s"Started worker container : $containerId")
      val newYarnClusterStats = updateContainer(containerId, yarnClusterStats)

      if(needMoreWorkers(newYarnClusterStats)) {
        stay using newYarnClusterStats
      } else {
        startServicesActor(newYarnClusterStats)
        goto(AwaitingTermination) using newYarnClusterStats
      }

  }

  private def updateContainer(containerId: ContainerId, yarnClusterStats: YarnClusterStats): YarnClusterStats = {
    val runningContainerStatus = ContainerStatus.newInstance(containerId, ContainerState.RUNNING, "", 0)
    val newYarnClusterStats =
      yarnClusterStats.withUpdatedContainer(runningContainerStatus) getOrElse {
        LOG.warn("no allocated worker container found ${container.getId}")
        yarnClusterStats
      }
    newYarnClusterStats
  }

  private def startServicesActor(newYarnClusterStats: YarnClusterStats): Unit = {
    servicesActor match {
      case None =>
        val masters = newYarnClusterStats.getRunningMasterContainersAddrs
        servicesActor = Option(context.actorOf(Props(classOf[ServicesLauncherActor], masters, host, servicesPort)))
      case _ =>
    }
  }

  private def needMoreWorkers(newYarnClusterStats: YarnClusterStats): Boolean = {
    appConfig.getEnv(WORKER_CONTAINERS).toInt > newYarnClusterStats.getRunningContainersCount(WORKER)
  }

  private def needMoreMasters(newYarnClusterStats: YarnClusterStats): Boolean = {
    appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt > newYarnClusterStats.getRunningContainersCount(MASTER)
  }

  /**
   * Filters out containers that are already allocated and emits warning.
   */
  private def filterAllocatedAndWarn(yarnClusterStats: YarnClusterStats)(c:Container): Boolean = {
    yarnClusterStats.getContainerInfo(c.getId) match {
      case Some(container) =>
        LOG.info("warning already allocated ${container.getId}")
        false
      case None =>
        true
    }
  }

  private def launchMasterContainer(yarnClusterStats:YarnClusterStats, container: Container): YarnClusterStats = {
    val host = container.getNodeId.getHost
    val port = container.getNodeId.getPort
    val masterCommand = MasterContainerCommand(appConfig, HostPort(host, port)).getCommand
    launchCommand(container, masterCommand)
    yarnClusterStats.withContainer(newContainerInfoInstance(container, MASTER))
  }

  private def launchWorkerContainer(masterAddr: HostPort)(yarnClusterStats:YarnClusterStats, container: Container): YarnClusterStats = {
    val workerHost = container.getNodeId.getHost
    val workerCommand = WorkerContainerCommand(appConfig, masterAddr, workerHost).getCommand
    launchCommand(container, workerCommand)
    yarnClusterStats.withContainer(newContainerInfoInstance(container, WORKER))
  }

  private[master] def newContainerInfoInstance(container: Container, containerType: ContainerType): ContainerInfo = {
    val containerStatus = ContainerStatus.newInstance(container.getId, ContainerState.NEW, "", 0)
    ContainerInfo(container.getNodeId.getHost, container.getNodeId.getPort, containerType, containerStatus)
  }


  def containersCompletedHandler: StateFunction  = {
    case Event(containersCompleted@ContainersCompleted(containers), yarnClusterStats: YarnClusterStats) =>
      def handleCompletedContainers(statuses: List[ContainerStatus], stats: YarnClusterStats): YarnClusterStats = {
        statuses match {
          case status :: tail =>
            val newStats =
              stats.withUpdatedContainer(status)  getOrElse stats
            handleCompletedContainers(tail, newStats)
          case Nil => stats
        }
      }
      val newYarnClusterStats = handleCompletedContainers(containers, yarnClusterStats)
      if(areAllContainersCompleted(newYarnClusterStats))
        exit(newYarnClusterStats.containerStats)
      stay using newYarnClusterStats
  }

  private def areAllContainersCompleted(newYarnClusterStats: YarnClusterStats): Boolean = {
    (appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt == newYarnClusterStats.getCompletedContainersCount(MASTER)) &&
      (appConfig.getEnv(WORKER_CONTAINERS).toInt == newYarnClusterStats.getCompletedContainersCount(WORKER))
  }

  def terminalStateHandler: StateFunction = {
    case Event(state: RMTerminalState, yarnClusterStats: YarnClusterStats) =>
      LOG.info("Got Terminal State")
      terminate(yarnClusterStats.containerStats)
      stay
  }

  def unknownHandler: StateFunction = {
    case Event(unknown, _) =>
      LOG.info(s"Unknown message ${unknown.getClass.getName}")
      stay
  }


  private def launchCommand(container: Container, command: String) {
    LOG.info(s"Launching containter: containerId :  ${container.getId}, host ip : ${container.getNodeId.getHost}")
    LOG.info("Launching command : " + command)
    val containerContext = containerLaunchContextFactory(command)
    LOG.info(s"NodeManagerClient : $nodeManagerClient, container : $container, containerContext $containerContext")
    nodeManagerClient.startContainerAsync(container, containerContext)
  }

  private def requestWorkerContainers(): Unit = {
    val containers = (1 to appConfig.getEnv(WORKER_CONTAINERS).toInt).map(
      i => Resource.newInstance(appConfig.getEnv(WORKER_MEMORY).toInt, appConfig.getEnv(WORKER_VCORES).toInt)
    ).toList
    resourceManagerClient ! ContainersRequest(containers)
  }

  private def requestMasterContainers(registrationResponse: RegisterApplicationMasterResponse) = {
    //TODO remove that or do something more with that
    val previousContainersCount = registrationResponse.getContainersFromPreviousAttempts.size

    LOG.info(s"Previous container count : $previousContainersCount")
    if (previousContainersCount > 0) {
      LOG.warn("Previous container count > 0, can't do anything with it")
    }

    LOG.info(s"GEARPUMPMASTER_CONTAINERS: ${appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt}")
    val containers = (1 to appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt).map(
      i => Resource.newInstance(appConfig.getEnv(GEARPUMPMASTER_MEMORY).toInt, appConfig.getEnv(GEARPUMPMASTER_VCORES).toInt)
    ).toList
    resourceManagerClient ! ContainersRequest(containers)
  }

  private def terminate(containerStats: ContainerStats): Unit = {
    LOG.info(s"Application completed. $containerStats")

    nodeManagerClient.stop()
    resourceManagerClient ! PoisonPill
  }

  private def exit(containerStats: ContainerStats): Unit = {
    LOG.info(s"Application completed. $containerStats")

    nodeManagerClient.stop()
    resourceManagerClient ! AMShutdownRequest(containerStats)
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

  def apply(args: Array[String]) = {
    try {
      implicit val timeout = Timeout(5, TimeUnit.SECONDS)
      val config = ConfigFactory.parseResourcesAnySyntax(YARN_CONFIG)
      implicit val system = ActorSystem("GearPumpAM", config)
      val appConfig = new AppConfig(parse(args), config)
      val yarnConfiguration = new YarnConfiguration(new Configuration(true))
      LOG.info("HADOOP_CONF_DIR: " + System.getenv("HADOOP_CONF_DIR"))
      LOG.info("Yarn config (yarn.resourcemanager.hostname): " + yarnConfiguration.get("yarn.resourcemanager.hostname"))
      val amActorProps = Props(
        new YarnApplicationMaster(appConfig,
          yarnConfiguration,
          ResourceManagerClient.props(yarnConfiguration, appConfig),
          (yarnConf, am) => {
            val nmClient = new NMClientAsyncImpl(new NodeManagerCallbackHandler(am))
            nmClient.init(yarnConf)
            nmClient.start()
            nmClient
          },
          ContainerLaunchContext(yarnConfiguration, appConfig),
          YarnClusterStats(appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt,
                           appConfig.getEnv(WORKER_CONTAINERS).toInt)))
      system.actorOf(amActorProps, "GearPumpAMActor")
      system.awaitTermination()
      LOG.info("Shutting down")
      system.shutdown()
      val yarn = new ImmutableYarnClusterStats(Map.empty[ContainerId, ContainerInfo],
        1,
        1)
    } catch {
      case throwable: Throwable =>
        LOG.error("Caught exception", throwable)
        throwable.printStackTrace()
    }
  }

  apply(args)
}

