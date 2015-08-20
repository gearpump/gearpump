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

package io.gearpump.experiments.yarn.master

import akka.actor.ActorRef
import io.gearpump.experiments.yarn.AppConfig
import io.gearpump.util.LogUtil
import org.apache.hadoop.yarn.api.records.{Container, ContainerStatus, NodeReport}
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.slf4j.Logger

import scala.collection.JavaConverters._

class ResourceManagerCallbackHandler(appConfig: AppConfig, val resourceManagerClient: ActorRef) extends AMRMClientAsync.CallbackHandler {
  import AmActorProtocol._

  private val LOG: Logger = LogUtil.getLogger(getClass)

  def getProgress: Float = 0.5F

  def onContainersAllocated(allocatedContainers: java.util.List[Container]) {
    LOG.info(s"Got response from RM for container request, allocatedCnt=${allocatedContainers.size}")
    resourceManagerClient ! ContainersAllocated(allocatedContainers.asScala.toList)
  }

  def onContainersCompleted(completedContainers: java.util.List[ContainerStatus]) {
    LOG.info(s"Got response from RM. Completed containers=${completedContainers.size()}")
    resourceManagerClient ! ContainersCompleted(completedContainers.asScala.toList)
  }

  def onError(throwable: Throwable) {
    LOG.info("Error occurred")
    resourceManagerClient ! RMError(throwable)
  }

  def onNodesUpdated(updatedNodes: java.util.List[NodeReport]): Unit = {
    LOG.info("onNodesUpdates")
  }

  def onShutdownRequest() {
    LOG.info("Shutdown requested")
    resourceManagerClient ! RMShutdownRequest
  }

}
