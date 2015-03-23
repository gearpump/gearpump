package org.apache.gearpump.experiments.yarn.master

import akka.actor.ActorRef
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.api.records.ContainerStatus
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.yarn.api.records.NodeReport
import java.util.concurrent.atomic.AtomicInteger
import org.slf4j.Logger
import org.apache.hadoop.yarn.api.records.Container
import org.apache.gearpump.experiments.yarn.Actions._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.hadoop.yarn.api.records.ContainerExitStatus
import org.apache.gearpump.experiments.yarn.EnvVars._
import org.apache.gearpump.experiments.yarn.AppConfig

class ResourceManagerCallbackHandler(appConfig: AppConfig, am: ActorRef) extends AMRMClientAsync.CallbackHandler {
  val LOG: Logger = LogUtil.getLogger(getClass)
  val completedContainersCount = new AtomicInteger(0)
  val failedContainersCount = new AtomicInteger(0)
  val allocatedContainersCount = new AtomicInteger(0)
  val requestedContainersCount = new AtomicInteger(0)

  def getProgress: Float = completedContainersCount.get / appConfig.getEnv(WORKER_CONTAINERS).toInt

  def onContainersAllocated(allocatedContainers: java.util.List[Container]) {
    LOG.info(s"Got response from RM for container request, allocatedCnt=${allocatedContainers.size}")
    
    allocatedContainersCount.addAndGet(allocatedContainers.size)
    am ! LaunchContainers(allocatedContainers.asScala.toList)
  }

  def onContainersCompleted(completedContainers: java.util.List[ContainerStatus]) {
    LOG.info(s"Got response from RM for container request, completed containers=${completedContainers.size()}")
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
    val containerCount = appConfig.getEnv(WORKER_CONTAINERS).toInt + appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt
    val requestCount = containerCount - requestedContainersCount.get
    requestedContainersCount.addAndGet(requestCount)

    //todo:wont work with non uniform containers
    (0 until requestCount).foreach(request => {
      am ! ContainerRequestMessage(containerCount, appConfig.getEnv(GEARPUMPMASTER_VCORES).toInt)
    })

    if (completedContainersCount.get == containerCount) {
      LOG.info("completedContainersCount == containerCount, shutting down")
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
