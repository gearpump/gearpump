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

package org.apache.gearpump.experiments.yarn.appmaster

import java.util.concurrent.TimeUnit
import akka.actor._
import akka.util.Timeout
import com.typesafe.config.ConfigValueFactory
import org.apache.gearpump.cluster.ClientToMaster._
import org.apache.gearpump.cluster.ClusterConfig
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.gearpump.experiments.yarn.glue.Records._
import org.apache.gearpump.experiments.yarn.glue.{NMClient, RMClient, YarnConfig}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util._
import org.slf4j.Logger
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * YARN AppMaster. YARN AppMaster is responsible to start Gearpump masters, workers, UI server as
 * YARN containers.
 *
 * NOTE: It is different with Gearpump AppMaster. Gearpump AppMaster is a sub-process of worker.
 */
class YarnAppMaster(rmClient: RMClient, nmClient: NMClient,
    packagePath: String, hdfsConfDir: String,
    uiFactory: UIFactory)
  extends Actor {

  private val LOG: Logger = LogUtil.getLogger(getClass)
  private val akkaConf = context.system.settings.config
  private val servicesEnabled = akkaConf.getString(SERVICES_ENABLED).toBoolean
  private var uiStarted = false
  private val host = akkaConf.getString(Constants.GEARPUMP_HOSTNAME)

  private val port = Util.findFreePort().get

  private val trackingURL = "http://" + host + ":" + port

  // TODO: for now, only one master is supported.
  private val masterCount = 1
  private val masterMemory = akkaConf.getString(MASTER_MEMORY).toInt
  private val masterVCores = akkaConf.getString(MASTER_VCORES).toInt

  private var workerCount = akkaConf.getString(WORKER_CONTAINERS).toInt
  private val workerMemory = akkaConf.getString(WORKER_MEMORY).toInt
  private val workerVCores = akkaConf.getString(WORKER_VCORES).toInt

  val rootPath = System.getProperty(Constants.GEARPUMP_FULL_SCALA_VERSION)

  rmClient.start(self)
  nmClient.start(self)

  def receive: Receive = null

  private def registerAppMaster(): Unit = {
    val target = host + ":" + port
    rmClient.registerAppMaster(host, port, trackingURL)
  }

  registerAppMaster
  context.become(waitForAppMasterRegistered)

  import org.apache.gearpump.experiments.yarn.appmaster.YarnAppMaster._

  def waitForAppMasterRegistered: Receive = {
    case AppMasterRegistered =>
      LOG.info("YarnAppMaster registration completed")
      requestMasterContainers(masterCount)
      context.become(startingMasters(remain = masterCount, List.empty[MasterInfo]))
  }

  private def startingMasters(remain: Int, masters: List[MasterInfo]): Receive = box {
    case ContainersAllocated(containers) =>
      LOG.info(s"ContainersAllocated: containers allocated for master(remain=$remain), count: "
        + containers.size)
      val count = Math.min(containers.length, remain)
      val newMasters = (0 until count).toList.map { index =>
        val container = containers(index)
        MasterInfo(container.getId, container.getNodeId, launchMaster(container))
      }

      // Stops un-used containers
      containers.drop(count).map { container =>
        nmClient.stopContainer(container.getId, container.getNodeId)
      }

      context.become(startingMasters(remain, newMasters ++ masters))
    case ContainerStarted(containerId) =>
      LOG.info(s"ContainerStarted: container ${containerId} started for master(remain=$remain) ")
      if (remain > 1) {
        context.become(startingMasters(remain - 1, masters))
      } else {
        requestWorkerContainers(workerCount)
        context.become(startingWorkers(workerCount, masters, List.empty[WorkerInfo]))
      }
  }

  private def box(receive: Receive): Receive = {
    onError orElse receive orElse unHandled
  }

  private def startingWorkers(remain: Int, masters: List[MasterInfo], workers: List[WorkerInfo])
  : Receive = {
    box {
      case ContainersAllocated(containers) =>
        LOG.info(s"ContainersAllocated: containers allocated for workers(remain=$remain), " +
          s"count: " + containers.size)

        val count = Math.min(containers.length, remain)
        val newWorkers = (0 until count).toList.map { index =>
          val container = containers(index)
          launchWorker(container, masters)
          WorkerInfo(container.getId, container.getNodeId)
        }

        // Stops un-used containers
        containers.drop(count).map { container =>
          nmClient.stopContainer(container.getId, container.getNodeId)
        }
        context.become(startingWorkers(remain, masters, workers ++ newWorkers))
      case ContainerStarted(containerId) =>
        LOG.info(s"ContainerStarted: container $containerId started for worker(remain=$remain)")
        // The last one
        if (remain > 1) {
          context.become(startingWorkers(remain - 1, masters, workers))
        } else {
          if (servicesEnabled && !uiStarted) {
            context.actorOf(uiFactory.props(masters.map(_.host), host, port))
            uiStarted = true
          }
          context.become(service(effectiveConfig(masters.map(_.host)), masters, workers))
        }
    }
  }

  private def effectiveConfig(masters: List[HostPort]): Config = {
    val masterList = masters.map(pair => s"${pair.host}:${pair.port}")
    val config = context.system.settings.config
    config.withValue(Constants.GEARPUMP_CLUSTER_MASTERS,
      ConfigValueFactory.fromIterable(masterList.asJava))
  }

  private def onError: Receive = {
    case ContainersCompleted(containers) =>
      // TODO: we should recover the failed container from this...
      containers.foreach { status =>
        if (status.getExitStatus != 0) {
          LOG.error(s"ContainersCompleted: container ${status.getContainerId}" +
            s" failed with exit code ${status.getExitStatus}, msg: ${status.getDiagnostics}")
        } else {
          LOG.info(s"ContainersCompleted: container ${status.getContainerId} completed")
        }
      }
    case ShutdownApplication =>
      LOG.error("ShutdownApplication")
      nmClient.stop()
      rmClient.shutdownApplication()
      context.stop(self)
    case ResourceManagerException(ex) =>
      LOG.error("ResourceManagerException: " + ex.getMessage, ex)
      nmClient.stop()
      rmClient.failApplication(ex)
      context.stop(self)
    case Kill =>
      LOG.info("Kill: User asked to shutdown the application")
      sender ! CommandResult(success = true)
      self ! ShutdownApplication
  }

  private def service(config: Config, masters: List[MasterInfo], workers: List[WorkerInfo])
  : Receive = box {
    case GetActiveConfig(clientHost) =>
      LOG.info("GetActiveConfig: Get active configuration for client: " + clientHost)
      val filtered = ClusterConfig.filterOutDefaultConfig(
        config.withValue(Constants.GEARPUMP_HOSTNAME,
          ConfigValueFactory.fromAnyRef(clientHost)))
      sender ! ActiveConfig(filtered)
    case QueryVersion =>
      LOG.info("QueryVersion")
      sender ! Version(Util.version)
    case QueryClusterInfo =>
      LOG.info("QueryClusterInfo")
      val masterContainers = masters.map { master =>
        master.id.toString + s"(${master.nodeId.toString})"
      }

      val workerContainers = workers.map { worker =>
        worker.id.toString + s"(${worker.nodeId.toString})"
      }
      sender ! ClusterInfo(masterContainers, workerContainers)
    case AddMaster =>
      sender ! CommandResult(success = false, "Not Implemented")
    case RemoveMaster(masterId) =>
      sender ! CommandResult(success = false, "Not Implemented")
    case AddWorker(count) =>
      if (count == 0) {
        sender ! CommandResult(success = true)
      } else {
        LOG.info("AddWorker: Start to add new workers, count: " + count)
        workerCount += count
        requestWorkerContainers(count)
        context.become(startingWorkers(count, masters, workers))
        sender ! CommandResult(success = true)
      }
    case RemoveWorker(worker) =>
      val workerId = ContainerId.fromString(worker)
      LOG.info(s"RemoveWorker: remove worker $workerId")
      val info = workers.find(_.id.toString == workerId.toString)
      if (info.isDefined) {
        nmClient.stopContainer(info.get.id, info.get.nodeId)
        sender ! CommandResult(success = true)
        val remainWorkers = workers.filter(_.id != info.get.id)
        context.become(service(config, masters, remainWorkers))
      } else {
        sender ! CommandResult(success = false, "failed to find worker " + worker)
      }
  }

  private def unHandled: Receive = {
    case other =>
      LOG.info(s"Received unknown message $other")
  }

  private def requestMasterContainers(masters: Int) = {
    LOG.info(s"Request resource for masters($masters)")
    val containers = (1 to masters).map(
      i => Resource.newInstance(masterMemory, masterVCores)
    ).toList
    rmClient.requestContainers(containers)
  }

  private def launchMaster(container: Container): HostPort = {
    LOG.info(s"Launch Master on container " + container.getNodeHttpAddress)
    val host = container.getNodeId.getHost

    val port = Util.findFreePort().get

    LOG.info("=============PORT" + port)
    val masterCommand = MasterCommand(akkaConf, rootPath, HostPort(host, port))
    nmClient.launchCommand(container, masterCommand.get, packagePath, hdfsConfDir)
    HostPort(host, port)
  }

  private def requestWorkerContainers(workers: Int): Unit = {
    LOG.info(s"Request resource for workers($workers)")
    val containers = (1 to workers).map(
      i => Resource.newInstance(workerMemory, workerVCores)
    ).toList

    rmClient.requestContainers(containers)
  }

  private def launchWorker(container: Container, masters: List[MasterInfo]): Unit = {
    LOG.info(s"Launch Worker on container " + container.getNodeHttpAddress)
    val workerHost = container.getNodeId.getHost
    val workerCommand = WorkerCommand(akkaConf, rootPath, masters.head.host, workerHost)
    nmClient.launchCommand(container, workerCommand.get, packagePath, hdfsConfDir)
  }
}

object YarnAppMaster extends AkkaApp with ArgumentsParser {
  val LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "conf" -> CLIOption[String]("<Gearpump configuration directory on HDFS>", required = true),
    "package" -> CLIOption[String]("<Gearpump package path on HDFS>", required = true)
  )

  override def akkaConfig: Config = {
    ClusterConfig.ui()
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    implicit val timeout = Timeout(5, TimeUnit.SECONDS)
    implicit val system = ActorSystem("GearpumpAM", akkaConf)

    val yarnConf = new YarnConfig()

    val confDir = parse(args).getString("conf")
    val packagePath = parse(args).getString("package")

    LOG.info("HADOOP_CONF_DIR: " + System.getenv("HADOOP_CONF_DIR"))
    LOG.info("YARN Resource Manager: " + yarnConf.resourceManager)

    val rmClient = new RMClient(yarnConf)
    val nmClient = new NMClient(yarnConf, akkaConf)
    val appMaster = system.actorOf(Props(new YarnAppMaster(rmClient,
      nmClient, packagePath, confDir, UIService)))

    val daemon = system.actorOf(Props(new Daemon(appMaster)))
    Await.result(system.whenTerminated, Duration.Inf)
    LOG.info("YarnAppMaster is shutdown")
  }

  class Daemon(appMaster: ActorRef) extends Actor {
    context.watch(appMaster)

    override def receive: Actor.Receive = {
      case Terminated(actor) =>
        if (actor.compareTo(appMaster) == 0) {
          LOG.info(s"YarnAppMaster ${appMaster.path.toString} is terminated, " +
            s"shutting down current ActorSystem")
          context.system.terminate()
          context.stop(self)
        }
    }
  }

  case class ResourceManagerException(throwable: Throwable)
  case object ShutdownApplication
  case class ContainersRequest(containers: List[Resource])
  case class ContainersAllocated(containers: List[Container])
  case class ContainersCompleted(containers: List[ContainerStatus])
  case class ContainerStarted(containerId: ContainerId)
  case object AppMasterRegistered

  case class GetActiveConfig(clientHost: String)

  case object QueryClusterInfo
  case class ClusterInfo(masters: List[String], workers: List[String]) {
    override def toString: String = {
      val separator = "\n"
      val masterSection = "masters: " + separator + masters.mkString("\n") + "\n"

      val workerSection = "workers: " + separator + workers.mkString("\n") + "\n"
      masterSection + workerSection
    }
  }

  case object Kill
  case class ActiveConfig(config: Config)

  case object QueryVersion

  case class Version(version: String)

  case class MasterInfo(id: ContainerId, nodeId: NodeId, host: HostPort)

  case class WorkerInfo(id: ContainerId, nodeId: NodeId)

  def getAppMaster(report: ApplicationReport, system: ActorSystem): ActorRef = {
    import org.apache.gearpump.experiments.yarn.client.AppMasterResolver

    AppMasterResolver.resolveAppMasterAddress(report, system)
  }
}