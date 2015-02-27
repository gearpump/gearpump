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
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.slf4j.Logger
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.ContainerStatus
import org.apache.hadoop.yarn.api.records.NodeReport
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.api.records.ContainerExitStatus
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import java.util.concurrent.ConcurrentLinkedQueue

class RMCallbackHandler(nmClientAsync:NMClientAsync, 
                                containerListener:NMCallbackHandler,
                                am: ApplicationMaster) extends AMRMClientAsync.CallbackHandler {
  
  val LOG: Logger = LogUtil.getLogger(getClass)
  val completedContainersCount = new AtomicInteger(0)
  val failedContainersCount = new AtomicInteger(0)
  val allocatedContainersCount = new AtomicInteger(0)
  val requestedContainersCount = new AtomicInteger(0)
  val launcherThreads = new ConcurrentLinkedQueue[Thread]
  def getAMArgs = am.getamArgs
  
  @volatile private var done = false
  def isDone = done

	def getProgress: Float = {
	  completedContainersCount.get()/getAMArgs.containers
  }

  def onContainersAllocated(allocatedContainers : java.util.List[Container]){
	LOG.info("Got response from RM for container request, allocatedCnt="
			+ allocatedContainers.size());
	allocatedContainersCount.addAndGet(allocatedContainers.size());

  //TODO: use future instead of Thread
	for (allocatedContainer <- allocatedContainers.asScala) {
		val runnableLaunchContainer = new ContainerLauncher(allocatedContainer, nmClientAsync, containerListener)    
		val launchThread = new Thread(runnableLaunchContainer);
		launcherThreads.add(launchThread);
		launchThread.start();
	}
}


def onContainersCompleted(completedContainers: java.util.List[ContainerStatus]) {
	LOG.info(s"Got response from RM for container request, completed containers=$completedContainers.size()");

	for (containerStatus <- completedContainers.asScala) {
		var exitStatus = containerStatus.getExitStatus()
				LOG.info(s"ContainerID=$containerStatus.getContainerId(), state=$containerStatus.getState(), exitStatus=$exitStatus")

				if(exitStatus != 0) {
					//if container failed
					if(exitStatus == ContainerExitStatus.ABORTED) {
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

	}

	// request more containers if any failed
	val requestCount = getAMArgs.containers - requestedContainersCount.get();
	requestedContainersCount.addAndGet(requestCount);

	for (i <- 0 until requestCount) {
		am.addContainerRequest(getAMArgs.containerMemory, getAMArgs.containerVCores)
	}

	if (completedContainersCount.get() == getAMArgs.containers) {
		done = true;
	}

}

def onError(throwable: Throwable)  {
	LOG.info("Error occurred")
	done = true;
}

def onNodesUpdated(updatedNodes: java.util.List[NodeReport]) {}

def onShutdownRequest()  {
	LOG.info("Shutdown requested")        
	done = true
}

private[RMCallbackHandler] class ContainerLauncher(container: Container, nmClientAsync:NMClientAsync, containerListener: NMCallbackHandler) extends Runnable {
  def run() {
    val command: List[String] = List(
        "/bin/date",
        "1>/tmp/panchostdout",
        "2>/tmp/panchostderr")

        val ctx = ContainerLaunchContext.newInstance(Map[String, LocalResource]().asJava, 
            Map[String, String]().asJava, 
            command.asJava, 
            null, 
            null, 
            null);
   
  nmClientAsync.startContainerAsync(container, ctx);   
  }
 }


}

