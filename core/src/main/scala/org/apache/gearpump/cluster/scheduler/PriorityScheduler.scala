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

package org.apache.gearpump.cluster.scheduler

import org.apache.gearpump.cluster.AppMasterToMaster.RequestResource
import org.apache.gearpump.cluster.MasterToAppMaster.ResourceAllocated
import org.apache.gearpump.cluster.WorkerInfo
import org.apache.gearpump.cluster.scheduler.Scheduler.PendingRequest
import org.slf4j.{LoggerFactory, Logger}

import scala.collection.mutable

class PriorityScheduler extends Scheduler{
  private val LOG: Logger = LoggerFactory.getLogger(classOf[PriorityScheduler])

  private val resourceRequests = new mutable.PriorityQueue[PendingRequest]()(requestOrdering)

  def requestOrdering = new Ordering[PendingRequest] {
    override def compare(x: PendingRequest, y: PendingRequest) = {
      var res = x.request.priority.id - y.request.priority.id
      if(res == 0)
        res = y.timeStamp.compareTo(x.timeStamp)
      res
    }
  }

  override def receive: Receive = super.handleScheduleMessage orElse resourceRequestHandler

  override def allocateResource(): Unit = {
    val length = resources.size

    val flattenResource = resources.toArray.zipWithIndex.flatMap((workerWithIndex) => {
      val ((worker, resource), index) = workerWithIndex
      0.until(resource.slots).map((seq) => (worker, seq * length + index))
    }).sortBy(_._2).map(_._1)

    val total = Resource(flattenResource.length)
    var tempStoredRequests = Array.empty[PendingRequest]
    def assignResourceToApplication(allocated : Resource) : Unit = {
      if (allocated == total || resourceRequests.isEmpty) {
        return
      }
      val pendingRequest = resourceRequests.dequeue()
      val (appMaster, request, timeStamp) = (pendingRequest.appMaster, pendingRequest.request, pendingRequest.timeStamp)
      if (request.worker != null && resources.exists(_._1.id == request.worker.id)) {
        val workerResource = resources.find(_._1.id == request.worker.id).getOrElse((WorkerInfo(-1), Resource(0)))
        if (workerResource._2.greaterThan(request.resource)) {
          appMaster ! ResourceAllocated(Array(ResourceAllocation(request.resource, workerResource._1)))
          assignResourceToApplication(allocated.add(request.resource))
        } else {
          tempStoredRequests = tempStoredRequests :+ pendingRequest
          assignResourceToApplication(allocated)
        }
      }

      val newAllocated = Resource.min(total.subtract(allocated), request.resource)
      val singleAllocation = flattenResource.slice(allocated.slots, allocated.add(newAllocated).slots)
        .groupBy((actor) => actor).mapValues(_.length).toArray.map((resource) =>  ResourceAllocation(Resource(resource._2), resource._1))
      appMaster ! ResourceAllocated(singleAllocation)
      if (request.resource.greaterThan(newAllocated)) {
        tempStoredRequests = tempStoredRequests :+ new PendingRequest(appMaster, ResourceRequest(request.resource.subtract(newAllocated), request.priority, request.worker), timeStamp)
      }
      assignResourceToApplication(allocated.add(newAllocated))
    }
    assignResourceToApplication(Resource(0))

    for(request <- tempStoredRequests)
      resourceRequests.enqueue(request)
  }

  def resourceRequestHandler: Receive = {
    case RequestResource(appId, requests) =>
      requests.foreach(request => {
        LOG.info(s"Request resource: appId: $appId, slots: ${request.resource.slots}")
        val appMaster = sender()
        resourceRequests.enqueue(new PendingRequest(appMaster, request, System.currentTimeMillis()))
      })
      allocateResource()
  }

}
