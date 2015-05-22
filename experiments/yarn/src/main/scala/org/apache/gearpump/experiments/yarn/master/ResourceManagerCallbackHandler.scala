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

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorRef
import org.apache.gearpump.experiments.yarn.AppConfig
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.yarn.api.records.{Container, ContainerExitStatus, ContainerStatus, NodeReport}
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.slf4j.Logger

import scala.collection.JavaConverters._

class ResourceManagerCallbackHandler(appConfig: AppConfig, val resourceManagerClient: ActorRef) extends AMRMClientAsync.CallbackHandler {
  import AmActorProtocol._

  private val LOG: Logger = LogUtil.getLogger(getClass)
  private val completedContainersCount = new AtomicInteger(0)
  private val failedContainersCount = new AtomicInteger(0)
  private val allocatedContainersCount = new AtomicInteger(0)
  private val requestedContainersCount = new AtomicInteger(0)

  def getProgress: Float = completedContainersCount.get / appConfig.getEnv(WORKER_CONTAINERS).toInt

  def onContainersAllocated(allocatedContainers: java.util.List[Container]) {
    LOG.info(s"Got response from RM for container request, allocatedCnt=${allocatedContainers.size}")
    
    allocatedContainersCount.addAndGet(allocatedContainers.size)
    resourceManagerClient ! ContainersAllocated(allocatedContainers.asScala.toList)
  }

  def onContainersCompleted(completedContainers: java.util.List[ContainerStatus]) {
    LOG.info(s"Got response from RM. Completed containers=${completedContainers.size()}")
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
    })

    // request more containers if any failed
    val containerCount = appConfig.getEnv(WORKER_CONTAINERS).toInt + appConfig.getEnv(GEARPUMPMASTER_CONTAINERS).toInt
    val requestCount = containerCount - requestedContainersCount.get
    requestedContainersCount.addAndGet(requestCount)

    //todo:wont work with non uniform containers
    (0 until requestCount).foreach(request => {
      resourceManagerClient ! AdditionalContainersRequest(containerCount)
    })

    if (completedContainersCount.get == containerCount) {
      LOG.info("completedContainersCount == containerCount, shutting down")
      resourceManagerClient ! RMAllRequestedContainersCompleted(ContainerStats(allocatedContainersCount.get, completedContainersCount.get, failedContainersCount.get))
    }

  }

  def onError(throwable: Throwable) {
    LOG.info("Error occurred")
    resourceManagerClient ! RMError(throwable, ContainerStats(allocatedContainersCount.get, completedContainersCount.get, failedContainersCount.get))
  }

  def onNodesUpdated(updatedNodes: java.util.List[NodeReport]): Unit = {
    LOG.info("onNodesUpdates")
  }

  def onShutdownRequest() {
    LOG.info("Shutdown requested")
    resourceManagerClient ! RMShutdownRequest(ContainerStats(allocatedContainersCount.get, completedContainersCount.get, failedContainersCount.get))
  }

}
