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

import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.apache.hadoop.yarn.api.records.ContainerId
import java.nio.ByteBuffer
import org.apache.hadoop.yarn.api.records.ContainerStatus
import org.slf4j.Logger
import org.apache.gearpump.util.LogUtil


private class NMCallbackHandler(am: ApplicationMaster) extends NMClientAsync.CallbackHandler {
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

