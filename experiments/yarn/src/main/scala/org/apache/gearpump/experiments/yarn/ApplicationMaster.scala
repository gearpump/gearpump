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

import org.apache.gearpump.cluster.main.ArgumentsParser
import org.apache.gearpump.cluster.main.CLIOption
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.NMClient
import org.apache.hadoop.yarn.util.Records
import org.apache.hadoop.yarn.api.records.Priority
import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.slf4j.Logger
import org.apache.gearpump.util.LogUtil
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.apache.hadoop.yarn.api.records.ContainerId
import java.nio.ByteBuffer
import org.apache.hadoop.yarn.api.records.ContainerStatus
import java.util.concurrent.atomic.AtomicInteger
import org.apache.hadoop.yarn.api.records.Container
import scala.collection.JavaConverters._
import org.apache.hadoop.yarn.api.records.NodeReport
import org.apache.hadoop.yarn.api.records.ContainerExitStatus
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus

/**
 * Yarn ApplicationMaster. 
 */
class ApplicationMaster(amArgs: ApplicationMasterArguments, yarnConf : YarnConfiguration) {

import org.apache.gearpump.experiments.yarn.ApplicationMaster._

	private val LOG: Logger = LogUtil.getLogger(getClass)  
  private val ONE_SEC_INTERVAL = 1000
  private var amRMClient: AMRMClientAsync[ContainerRequest] = _
  private var nmCallbackHandler: NMCallbackHandler = _
  private var nmClientAsync: NMClientAsync = _ 
  private var rmCallbackHandler: RMCallbackHandler = _
  
  def getYarnConf = yarnConf
  def getamArgs = amArgs
  

private def getAMRMClient(rmCallbackHandler: RMCallbackHandler): AMRMClientAsync[ContainerRequest] = {
	val amrmClient:AMRMClientAsync[ContainerRequest] = AMRMClientAsync.createAMRMClientAsync(ONE_SEC_INTERVAL, rmCallbackHandler)
	amrmClient.init(getYarnConf)
	amrmClient.start
	amrmClient
}

private def getNMClient(containerListener:NMCallbackHandler) = {
	val nmClient = new NMClientAsyncImpl(containerListener)
	nmClient.init(getYarnConf);
	nmClient.start();
	nmClient    
}

private def registerAm:RegisterApplicationMasterResponse = {
	val response = amRMClient.registerApplicationMaster("", 0, "")

			LOG.info(s"Max mem and vcores capabilities : " + 
					response.getMaximumResourceCapability.getMemory + " and " + 
					response.getMaximumResourceCapability.getVirtualCores)
  response
}

private def requestContainers(registrationResponse: RegisterApplicationMasterResponse) {
	val previousContainersCount = registrationResponse.getContainersFromPreviousAttempts().size()
			LOG.info(s"Previous container count : $previousContainersCount")
			val containersToRequestCount = amArgs.containers - previousContainersCount;
  
	for(x <- 1 to containersToRequestCount) {
		    val containerRequest = createContainerRequest(amArgs.containerMemory, amArgs.containerVCores)
				LOG.info(s"Requested container : $containerRequest")
			  amRMClient.addContainerRequest(containerRequest)
	}
}


def run:Boolean = {
    LOG.info("Initializing application master")
    initialize    
    
    LOG.info("Requesting containers")
    requestContainers(registerAm)
    
    LOG.info("Stopping application master")
    waitForContainers
    waitForLauncherThreads
    cleanUp 
  }

  private def initialize {
    nmCallbackHandler = createNMCallbackHandler
    nmClientAsync = getNMClient(nmCallbackHandler)
    rmCallbackHandler = new RMCallbackHandler(nmClientAsync, nmCallbackHandler, this)
    amRMClient = getAMRMClient(rmCallbackHandler)

  }

//TODO: refactor, future instead of thread
 private def waitForContainers {
   while (!rmCallbackHandler.isDone && (rmCallbackHandler.completedContainersCount.get() != amArgs.containers)) {
	   Thread.sleep(200);
   }
  }
 
  //TODO: future instead of Thread
  private def waitForLauncherThreads {
    rmCallbackHandler.launcherThreads.asScala.foreach { t => t.join(10000) }  
  }
  
  private def cleanUp:Boolean = {
    LOG.info("Application completed. Stopping running containers");
    nmClientAsync.stop();
    var success = true
    
    if (rmCallbackHandler.failedContainersCount.get() == 0 && 
        rmCallbackHandler.completedContainersCount.get() == amArgs.containers) {
        amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Ok", null);
    } else {
      
      val failMessage = "Diagnostics." + ", total=" + amArgs.containers +
          ", completed=" + rmCallbackHandler.completedContainersCount.get() + ", allocated=" +
          rmCallbackHandler.allocatedContainersCount.get() + ", failed=" +
          rmCallbackHandler.failedContainersCount.get();
          amRMClient.unregisterApplicationMaster(FinalApplicationStatus.FAILED, failMessage, null);
      
       success = false;
    }
    amRMClient.stop
    success
  }

 private def createNMCallbackHandler:NMCallbackHandler = {
 			new NMCallbackHandler(this);
		}

 
 private def createContainerRequest(memory:Int, vCores:Int):ContainerRequest = {
    val priorityRecord = Records.newRecord(classOf[Priority])
    priorityRecord.setPriority(0)
    val priority = Priority.newInstance(0)
    val capability = Resource.newInstance(memory, vCores)
    new ContainerRequest(capability, null, null, priority)
  }

  def addContainerRequest(memory: Int, vCores: Int) {
        val containerRequest = createContainerRequest(memory, vCores)
        LOG.info(s"Requested container : $containerRequest")
        amRMClient.addContainerRequest(containerRequest)
 }

}

object ApplicationMaster extends ArgumentsParser {
 
	override val options: Array[(String, CLIOption[Any])] = Array(
			"containers" -> CLIOption[Int]("<Required number of containers>", required = false, defaultValue = Some(1)),
      "containerMemory" -> CLIOption[Int]("<Required amount of memory for container>", required = false, defaultValue = Some(1024)),
      "containerVCores" -> CLIOption[Int]("<Required number of vcores for container>", required = false, defaultValue = Some(1))
			)

			def main(args: Array[String]) = {
	      val result = new ApplicationMaster(new ApplicationMasterArguments(parse(args)), new YarnConfiguration).run
        if(!result) {
          System.exit(1)  
        }
      }
}
