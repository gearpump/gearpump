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

package io.gearpump.experiments.yarn.glue

import java.nio.ByteBuffer

import akka.actor.ActorRef
import com.typesafe.config.Config
import org.apache.hadoop.yarn.api.records.{ApplicationId => YarnApplicationId, ApplicationReport => YarnApplicationReport, Container => YarnContainer, ContainerId => YarnContainerId, ContainerStatus => YarnContainerStatus, NodeId => YarnNodeId, Resource => YarnResource}
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl

import io.gearpump.experiments.yarn.appmaster.YarnAppMaster.ContainerStarted
import io.gearpump.experiments.yarn.glue.Records._
import io.gearpump.util.LogUtil
/**
 * Adapter for node manager client
 */
class NMClient(yarnConf: YarnConfig, config: Config) extends NMClientAsync.CallbackHandler {

  private val LOG = LogUtil.getLogger(getClass)

  private var reportTo: ActorRef = null
  private var client: NMClientAsyncImpl = null

  def start(reportTo: ActorRef): Unit = {
    LOG.info("Starting Node Manager Client NMClient...")
    this.reportTo = reportTo
    client = new NMClientAsyncImpl(this)
    client.init(yarnConf.conf)
    client.start()
  }

  private[glue]
  override def onContainerStarted(
      containerId: YarnContainerId, allServiceResponse: java.util.Map[String, ByteBuffer]) {
    LOG.info(s"Container started : $containerId, " + allServiceResponse)
    reportTo ! ContainerStarted(containerId)
  }

  private[glue]
  override def onContainerStatusReceived(
      containerId: YarnContainerId, containerStatus: YarnContainerStatus) {
    LOG.info(s"Container status received : $containerId, status $containerStatus")
  }

  private[glue]
  override def onContainerStopped(containerId: YarnContainerId) {
    LOG.error(s"Container stopped : $containerId")
  }

  private[glue]
  override def onGetContainerStatusError(containerId: YarnContainerId, throwable: Throwable) {
    LOG.error(s"Container exception : $containerId", throwable)
  }

  private[glue]
  override def onStartContainerError(containerId: YarnContainerId, throwable: Throwable) {
    LOG.error(s"Container exception : $containerId", throwable)
  }

  private[glue]
  override def onStopContainerError(containerId: YarnContainerId, throwable: Throwable) {
    LOG.error(s"Container exception : $containerId", throwable)
  }

  def launchCommand(
      container: Container, command: String, packagePath: String, configPath: String): Unit = {
    LOG.info(s"Launching command : $command on container" +
      s":  ${container.getId}, host ip : ${container.getNodeId.getHost}")
    val context = ContainerLaunchContext(yarnConf.conf, command, packagePath, configPath)
    client.startContainerAsync(container, context)
  }

  def stopContainer(containerId: ContainerId, nodeId: NodeId): Unit = {
    LOG.info(s"Stop container ${containerId.toString} on node: ${nodeId.toString} ")
    client.stopContainerAsync(containerId, nodeId)
  }

  def stop(): Unit = {
    LOG.info(s"Shutdown NMClient")
    client.stop()
  }
}