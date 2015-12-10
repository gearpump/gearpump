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

package io.gearpump.experiments.yarn.glue

import akka.actor.ActorRef
import io.gearpump.experiments.yarn.appmaster.YarnAppMaster.{AppMasterRegistered, ContainersAllocated, ContainersCompleted, ResourceManagerException, ShutdownApplication}
import io.gearpump.experiments.yarn.glue.Records._
import io.gearpump.util.LogUtil
import org.apache.hadoop.yarn.api.records.{ApplicationId => YarnApplicationId, ApplicationReport => YarnApplicationReport, Container => YarnContainer, ContainerId => YarnContainerId, ContainerStatus => YarnContainerStatus, FinalApplicationStatus, NodeId => YarnNodeId, NodeReport, Priority, Resource => YarnResource}
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.slf4j.Logger

import scala.collection.JavaConverters._

/**
 * Adapter for resource manager client
 */
class RMClient(yarnConf: YarnConfig) extends AMRMClientAsync.CallbackHandler {

  private val LOG: Logger = LogUtil.getLogger(getClass)

  private var reportTo: ActorRef = null
  private var client: AMRMClientAsync[ContainerRequest] = null

  def start(reportTo: ActorRef): Unit = {
    LOG.info("Starting Resource Manager Client RMClient...")
    this.reportTo = reportTo
    client = startAMRMClient
  }

  private def startAMRMClient: AMRMClientAsync[ContainerRequest] = {
    val timeIntervalMs = 1000 //ms
    val amrmClient = AMRMClientAsync.createAMRMClientAsync[ContainerRequest](timeIntervalMs, this)
    amrmClient.init(yarnConf.conf)
    amrmClient.start()
    amrmClient
  }

  override def getProgress: Float = 0.5F

  private var allocatedContainers = Set.empty[YarnContainerId]

  private[glue]
  override def onContainersAllocated(containers: java.util.List[YarnContainer]) {
    val newContainers = containers.asScala.toList.filterNot(container => allocatedContainers.contains(container.getId))
    allocatedContainers ++= newContainers.map(_.getId)
    LOG.info(s"New allocated ${newContainers.size} containers")
    reportTo ! ContainersAllocated(newContainers.map(yarnContainerToContainer(_)))
  }

  private[glue]
  override def onContainersCompleted(completedContainers: java.util.List[YarnContainerStatus]): Unit = {
    LOG.info(s"Got response from RM. Completed containers=${completedContainers.size()}")
    reportTo ! ContainersCompleted(completedContainers.asScala.toList.map(yarnContainerStatusToContainerStatus(_)))
  }

  private[glue]
  override def onError(ex: Throwable) {
    LOG.info("Error occurred")
    reportTo ! ResourceManagerException(ex)
  }

  private[glue]
  override def onNodesUpdated(updatedNodes: java.util.List[NodeReport]): Unit = {
    LOG.info("onNodesUpdates")
  }

  private[glue]
  override def onShutdownRequest() {
    LOG.info("Shutdown requested")
    reportTo ! ShutdownApplication
  }

  def requestContainers(containers: List[Resource]): Unit = {
    LOG.info(s"request Resource, slots: ${containers.length}, ${containers.mkString("\n")}")
    containers.foreach(resource => {
      client.addContainerRequest(createContainerRequest(resource))
    })
  }

  private def createContainerRequest(capability: Resource): ContainerRequest = {
    LOG.info("creating ContainerRequest")
    val priorityRecord = org.apache.hadoop.yarn.util.Records.newRecord(classOf[Priority])
    priorityRecord.setPriority(0)
    val priority = Priority.newInstance(0)
    new ContainerRequest(capability, null, null, priority)
  }

  def registerAppMaster(host: String, port: Int, url: String): Unit = {
    LOG.info(s"Received RegisterAMMessage! $host:$port $url")
    val response = client.registerApplicationMaster(host, port, url)
    LOG.info("Received RegisterAppMasterResponse ")
    reportTo ! AppMasterRegistered
  }

  def shutdownApplication(): Unit = {
    LOG.info(s"Shutdown application")
    client.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "success", null)
    client.stop()
  }

  def failApplication(ex: Throwable): Unit = {
    LOG.error(s"Application failed! ", ex)
    client.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, ex.getMessage, null)
    client.stop()
  }
}
