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

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.main.{ParseResult, ArgumentsParser, CLIOption}
import org.apache.gearpump.experiments.yarn.Actions._
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import org.slf4j.Logger
import scala.collection.JavaConverters._
import org.apache.gearpump.experiments.yarn.Client._
import org.apache.gearpump.experiments.yarn.CmdLineVars._
import org.apache.gearpump.experiments.yarn.EnvVars._



object Actions {
  sealed trait Reason
  case class Failed(throwable: Throwable) extends Reason
  case object ShutdownRequest extends Reason
  case object AllRequestedContainersCompleted extends Reason

  case class LaunchContainers(containers: List[Container])
  case class ContainerRequestMessage(memory: Int, vCores: Int)
  case class RMHandlerDone(reason: Reason, rMHandlerContainerStats: RMHandlerContainerStats)
  case class RMHandlerContainerStats(allocated: Int, completed: Int, failed: Int)
  case class RegisterAMMessage(appHostName: String, appHostPort: Int, appTrackingUrl: String)
  case class AMStatusMessage(appStatus: FinalApplicationStatus, appMessage: String, appTrackingUrl: String)
}

/**
 * Yarn ApplicationMaster.
 */
class YarnAMActor(gearConf: Configuration, yarnConf: YarnConfiguration) extends Actor {
  val LOG: Logger = LogUtil.getLogger(getClass)
  val nmCallbackHandler = createNMCallbackHandler
  val nmClientAsync = createNMClient(nmCallbackHandler)
  val rmCallbackHandler = context.actorOf(Props(classOf[RMCallbackHandlerActor], gearConf, self), "rmCallbackHandler")
  val amRMClient = context.actorOf(Props(classOf[AMRMClientAsyncActor], yarnConf, self), "amRMClient")
  
  
  override def receive: Receive = {
    case containerRequest: ContainerRequestMessage =>
      LOG.info("Received ContainerRequestMessage")
      amRMClient ! containerRequest
    case rmCallbackHandler: RMCallbackHandler =>
      LOG.info("Received RMCallbackHandler")
      amRMClient forward rmCallbackHandler
      amRMClient ! RegisterAMMessage("", 0, "")
    case amResponse: RegisterApplicationMasterResponse =>
      LOG.info("Received RegisterApplicationMasterResponse")
      requestContainers(amResponse)
    case launchContainers: LaunchContainers =>
      LOG.info("Received LaunchContainers")
      launchContainers.containers.foreach(container => {
        context.actorOf(Props(classOf[ContainerLauncherActor], container, nmClientAsync, nmCallbackHandler))
      })
    case done: RMHandlerDone =>
      cleanUp(done)
  }

  private[this] def createNMClient(containerListener: NMCallbackHandler): NMClientAsync = {
    LOG.info("Creating NMClientAsync")
    val nmClient = new NMClientAsyncImpl(containerListener)
    nmClient.init(yarnConf)
    nmClient.start()
    nmClient
  }

  private[this] def createNMCallbackHandler: NMCallbackHandler = {
    LOG.info("Creating NMCallbackHandler")
    NMCallbackHandler()
  }

  private[this] def requestContainers(registrationResponse: RegisterApplicationMasterResponse) {
    val previousContainersCount = registrationResponse.getContainersFromPreviousAttempts.size
    LOG.info(s"Previous container count : $previousContainersCount")
    val containersToRequestCount = gearConf.getEnv(CONTAINER_COUNT).toInt - previousContainersCount

    (1 to containersToRequestCount).foreach(requestId => {
      amRMClient ! ContainerRequestMessage(gearConf.getEnv(CONTAINER_MEMORY).toInt, gearConf.getEnv(CONTAINER_VCORES).toInt)
    })

  }

  private[this] def cleanUp(done: RMHandlerDone): Boolean = {
    LOG.info("Application completed. Stopping running containers")
    nmClientAsync.stop()
    var success = true

    val stats = done.rMHandlerContainerStats
    done.reason match {
      case failed: Failed =>
        val message = s"Failed. total=${gearConf.getEnv(CONTAINER_COUNT).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
        amRMClient ! AMStatusMessage(FinalApplicationStatus.FAILED, message, null)
        success = false
      case ShutdownRequest =>
        if (stats.failed == 0 && stats.completed == gearConf.getEnv(CONTAINER_COUNT).toInt) {
          val message = s"ShutdownRequest. total=${gearConf.getEnv(CONTAINER_COUNT).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
          amRMClient ! AMStatusMessage(FinalApplicationStatus.KILLED, message, null)
          success = false
        } else {
          val message = s"ShutdownRequest. total=${gearConf.getEnv(CONTAINER_COUNT).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
          amRMClient ! AMStatusMessage(FinalApplicationStatus.FAILED, message, null)
          success = false
        }
      case AllRequestedContainersCompleted =>
        val message = s"Diagnostics. total=${gearConf.getEnv(CONTAINER_COUNT).toInt}, completed=${stats.completed}, allocated=${stats.allocated}, failed=${stats.failed}"
        amRMClient ! AMStatusMessage(FinalApplicationStatus.SUCCEEDED, message, null)
        success = true
    }

    amRMClient ! PoisonPill
    success
  }
}

class NMCallbackHandler() extends NMClientAsync.CallbackHandler {
  val LOG: Logger = LogUtil.getLogger(getClass)

  def onContainerStarted(containerId: ContainerId, allServiceResponse: java.util.Map[String, ByteBuffer]) {
    LOG.info(s"Container started : $containerId")
  }

  def onContainerStatusReceived(containerId: ContainerId, containerStatus: ContainerStatus) {
    LOG.info(s"Container status received : $containerId, status $containerStatus")
  }

  def onContainerStopped(containerId: ContainerId) {
    LOG.info(s"Container stopped : $containerId")
  }

  def onGetContainerStatusError(containerId: ContainerId, throwable: Throwable) {
    LOG.error(s"Container exception : $containerId", throwable)
  }

  def onStartContainerError(containerId: ContainerId, throwable: Throwable) {
    LOG.error(s"Container exception : $containerId", throwable)
  }

  def onStopContainerError(containerId: ContainerId, throwable: Throwable) {
    LOG.error(s"Container exception : $containerId", throwable)
  }
}

object NMCallbackHandler {
  def apply() = new NMCallbackHandler()
}

class AMRMClientAsyncActor(yarnConf: YarnConfiguration, yarnAM: ActorRef) extends Actor {
  val LOG: Logger = LogUtil.getLogger(getClass)
  var client: AMRMClientAsync[ContainerRequest] = _

  private[this] def createContainerRequest(attrs: ContainerRequestMessage): ContainerRequest = {
    LOG.info("creating ContainerRequest")
    val priorityRecord = Records.newRecord(classOf[Priority])
    priorityRecord.setPriority(0)
    val priority = Priority.newInstance(0)
    val capability = Resource.newInstance(attrs.memory, attrs.vCores)
    new ContainerRequest(capability, null, null, priority)
  }

  private[this] def start(rmCallbackHandler: RMCallbackHandler): AMRMClientAsync[ContainerRequest] = {
    LOG.info("starting AMRMClientAsync")
    import YarnAM._
    val amrmClient: AMRMClientAsync[ContainerRequest] = AMRMClientAsync.createAMRMClientAsync(TIME_INTERVAL, rmCallbackHandler)
    amrmClient.init(yarnConf)
    amrmClient.start()
    amrmClient
  }

  override def preStart(): Unit = {
    LOG.info("preStart")
  }

  override def receive: Receive = {
    case rmCallbackHandler: RMCallbackHandler =>
      LOG.info("Received RMCallbackHandler")
      client = start(rmCallbackHandler)
    case containerRequest: ContainerRequestMessage =>
      LOG.info("Received ContainerRequestMessage")
      client.addContainerRequest(createContainerRequest(containerRequest))
    case amAttr: RegisterAMMessage =>
      LOG.info(s"Received RegisterAMMessage ${amAttr.appHostName} ${amAttr.appHostPort} ${amAttr.appTrackingUrl}")
      val response = client.registerApplicationMaster(amAttr.appHostName, amAttr.appHostPort, amAttr.appTrackingUrl)
      yarnAM ! response
    case amStatus: AMStatusMessage =>
      LOG.info("Received AMStatusMessage")
      client.unregisterApplicationMaster(amStatus.appStatus, amStatus.appMessage, amStatus.appTrackingUrl)
  }

}

class RMCallbackHandler(appConfig: Configuration, am: ActorRef) extends AMRMClientAsync.CallbackHandler {
  val LOG: Logger = LogUtil.getLogger(getClass)
  val completedContainersCount = new AtomicInteger(0)
  val failedContainersCount = new AtomicInteger(0)
  val allocatedContainersCount = new AtomicInteger(0)
  val requestedContainersCount = new AtomicInteger(0)

  def getProgress: Float = completedContainersCount.get / appConfig.getEnv(CONTAINER_COUNT).toInt

  def onContainersAllocated(allocatedContainers: java.util.List[Container]) {
    LOG.info(s"Got response from RM for container request, allocatedCnt=${allocatedContainers.size}")
    allocatedContainersCount.addAndGet(allocatedContainers.size)
    am ! LaunchContainers(allocatedContainers.asScala.toList)
  }

  def onContainersCompleted(completedContainers: java.util.List[ContainerStatus]) {
    LOG.info(s"Got response from RM for container request, completed containers=$completedContainers.size()")
    completedContainers.asScala.toList.foreach(containerStatus => {
      val exitStatus = containerStatus.getExitStatus
      LOG.info(s"ContainerID=$containerStatus.getContainerId(), state=$containerStatus.getState(), exitStatus=$exitStatus")

      if (exitStatus != 0) {
        //if container failed
        if (exitStatus == ContainerExitStatus.ABORTED) {
          allocatedContainersCount.decrementAndGet()
          requestedContainersCount.decrementAndGet()
        } else {
          //shell script failed
          completedContainersCount.incrementAndGet()
          failedContainersCount.incrementAndGet()
        }
      } else {
        completedContainersCount.incrementAndGet()
      }
      am ! RMHandlerContainerStats(allocatedContainersCount.get, completedContainersCount.get, failedContainersCount.get)
    })

    // request more containers if any failed
    val requestCount = appConfig.getEnv(CONTAINER_COUNT).toInt - requestedContainersCount.get
    requestedContainersCount.addAndGet(requestCount)

    (0 until requestCount).foreach(request => {
      am ! ContainerRequestMessage(appConfig.getEnv(CONTAINER_MEMORY).toInt, appConfig.getEnv(CONTAINER_VCORES).toInt)
    })

    if (completedContainersCount.get == appConfig.getEnv(CONTAINER_COUNT).toInt) {
      am ! RMHandlerDone(AllRequestedContainersCompleted, RMHandlerContainerStats(allocatedContainersCount.get, completedContainersCount.get, failedContainersCount.get))
    }

  }

  def onError(throwable: Throwable) {
    LOG.info("Error occurred")
    am ! RMHandlerDone(Failed(throwable), RMHandlerContainerStats(allocatedContainersCount.get, completedContainersCount.get, failedContainersCount.get))
  }

  def onNodesUpdated(updatedNodes: java.util.List[NodeReport]): Unit = {
    LOG.info("onNodesUpdates")
  }

  def onShutdownRequest() {
    LOG.info("Shutdown requested")
    am ! RMHandlerDone(ShutdownRequest, RMHandlerContainerStats(allocatedContainersCount.get, completedContainersCount.get, failedContainersCount.get))
  }

}

class RMCallbackHandlerActor(gearConf: Configuration, yarnAM: ActorRef) extends Actor {
  val LOG: Logger = LogUtil.getLogger(getClass)
  val rmCallbackHandler = new RMCallbackHandler(gearConf, yarnAM)

  override def preStart(): Unit = {
    LOG.info("Sending RMCallbackHandler to YarnAM")
    yarnAM ! rmCallbackHandler
  }

  override def receive: Receive = {
    case _ =>
      LOG.error(s"Unknown message received")
  }

}

class ContainerLauncherActor(container: Container, nmClientAsync: NMClientAsync, containerListener: NMCallbackHandler) extends Actor {
  val LOG: Logger = LogUtil.getLogger(getClass)

  override def preStart(): Unit = {
    launch(container)
  }

  override def receive: Receive = {
    case _ =>
      LOG.error(s"Unknown message received")
  }

  def launch(container: Container) {
    val command: List[String] = List(
      "/bin/date",
      "1>/tmp/panchostdout",
      "2>/tmp/panchostderr")

    val ctx = ContainerLaunchContext.newInstance(Map[String, LocalResource]().asJava,
      Map[String, String]().asJava,
      command.asJava,
      null,
      null,
      null)

    nmClientAsync.startContainerAsync(container, ctx)
  }
}


object YarnAM extends App with ArgumentsParser {
  val LOG: Logger = LogUtil.getLogger(getClass)
  val TIME_INTERVAL = 1000

  override val options: Array[(String, CLIOption[Any])] = Array(
    APPMASTER_IP -> CLIOption[String]("<Gearpump master ip>", required = false),
    APPMASTER_PORT -> CLIOption[String]("<Gearpump master port>", required = false)
  )

  def apply(args: Array[String]) = {
    try {
      implicit val timeout = Timeout(5, TimeUnit.SECONDS)
      val config = ConfigFactory.load
      implicit val system = ActorSystem("GearPumpAM", config)
      LOG.info("Creating YarnAMActor")
      system.actorOf(Props(classOf[YarnAMActor], new Configuration(parse(args), config), new YarnConfiguration), "GearPumpAMActor")
      system.awaitTermination()
      LOG.info("Shutting down")
      system.shutdown()
    } catch {
      case throwable: Throwable =>
        LOG.error("Caught exception", throwable)
    }

  }

  apply(args)

}
