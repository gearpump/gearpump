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
package org.apache.gearpump.scheduler

import org.apache.gearpump.cluster.MasterToAppMaster.ResourceAllocated

class FifoScheduler extends Scheduler{
  override def allocateResource(): Unit = {
    val length = resources.length
    val flattenResource = resources.zipWithIndex.flatMap((workerWithIndex) => {
      val ((worker, resource), index) = workerWithIndex
      0.until(resource.slots).map((seq) => (worker, seq * length + index))
    }).sortBy(_._2).map(_._1)

    val total = Resource(flattenResource.length)
    def assignResourceToApplication(allocated : Resource) : Unit = {
      if (allocated == total || resourceRequests.isEmpty) {
        return
      }

      val (appMaster, request) = resourceRequests.dequeue()
      if(request.isInstanceOf[Allocation])
        return
      val newAllocated = ResourceCalculator.min(ResourceCalculator.subtract(total, allocated), request)
      val singleAllocation = flattenResource.slice(allocated.slots, ResourceCalculator.add(allocated, newAllocated).slots)
        .groupBy((actor) => actor).mapValues(_.length).toArray.map((resource) =>  Allocation(resource._2, resource._1))
      appMaster ! ResourceAllocated(singleAllocation)
      if (ResourceCalculator.greaterThan(request, newAllocated)) {
        resourceRequests.enqueue((appMaster, ResourceCalculator.subtract(request, newAllocated)))
      }
      assignResourceToApplication(ResourceCalculator.add(allocated, newAllocated))
    }

    assignResourceToApplication(Resource(0))
  }
}
