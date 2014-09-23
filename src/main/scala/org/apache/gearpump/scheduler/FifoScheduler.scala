package org.apache.gearpump.scheduler
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

import org.apache.gearpump.cluster.MasterToAppMaster.ResourceAllocated

class FifoScheduler extends Scheduler{
  override def receive: Receive = super.handleScheduleMessage

  override def allocateResource(): Unit = {
    val length = resources.size
    val flattenResource = resources.toArray.zipWithIndex.flatMap((workerWithIndex) => {
      val ((worker, resource), index) = workerWithIndex
      0.until(resource.slots).map((seq) => (worker, seq * length + index))
    }).sortBy(_._2).map(_._1)

    val total = Resource(flattenResource.length)
    def assignResourceToApplication(allocated : Resource) : Unit = {
      if (allocated == total || resourceRequests.isEmpty) {
        return
      }

      val (appMaster, request) = resourceRequests.dequeue()
      val newAllocated = ResourceUtil.min(total.subtract(allocated), request.resource)
      val singleAllocation = flattenResource.slice(allocated.slots, allocated.add(newAllocated).slots)
        .groupBy((actor) => actor).mapValues(_.length).toArray.map((resource) =>  Allocation(Resource(resource._2), resource._1))
      appMaster ! ResourceAllocated(singleAllocation)
      if (request.resource.greaterThan(newAllocated)) {
        resourceRequests.enqueue((appMaster, ResourceRequest(request.resource.subtract(newAllocated), request.worker)))
      }
      assignResourceToApplication(allocated.add(newAllocated))
    }

    assignResourceToApplication(Resource(0))
  }
}
