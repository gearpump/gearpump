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

import akka.actor.ActorRef
import org.apache.gearpump.cluster.AppMasterToMaster.RequestResource
import org.apache.gearpump.cluster.MasterToAppMaster.ResourceAllocated
import org.apache.gearpump.cluster.scheduler.Scheduler.PendingRequest
import org.apache.gearpump.cluster.scheduler.Relaxation._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class PriorityScheduler extends Scheduler{

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
    var scheduleLater = Array.empty[PendingRequest]
    val resourcesSnapShot = resources.clone()
    var allocated = Resource.empty
    val totalResource = resourcesSnapShot.foldLeft(Resource.empty){ (totalResource, workerWithResource) =>
      val (_, (_, resource)) = workerWithResource
      totalResource.add(resource)
    }

    while(resourceRequests.nonEmpty && allocated.lessThan(totalResource)) {
      val PendingRequest(appMaster, request, timeStamp) = resourceRequests.dequeue()
      request.relaxation match {
        case ANY =>
          val newAllocated = allocateFairly(resourcesSnapShot, PendingRequest(appMaster, request, timeStamp))
          allocated = allocated.add(newAllocated)
        case ONEWORKER =>
          val availableResource = resourcesSnapShot.find{params =>
            val (_, (_, resource)) = params
            resource.greaterThan(request.resource)}
          if(availableResource.nonEmpty){
            val (workerId, (worker, resource)) = availableResource.get
            allocated = allocated.add(request.resource)
            appMaster ! ResourceAllocated(Array(ResourceAllocation(request.resource, worker, workerId)))
            resourcesSnapShot.update(workerId, (worker, resource.subtract(request.resource)))
          } else {
            scheduleLater = scheduleLater :+ PendingRequest(appMaster, request, timeStamp)
          }
        case SPECIFICWORKER =>
          if (resourcesSnapShot.contains(request.workerId)) {
            val (worker, availableResource) = resourcesSnapShot.get(request.workerId).get
            if (availableResource.greaterThan(request.resource)) {
              appMaster ! ResourceAllocated(Array(ResourceAllocation(request.resource, worker, request.workerId)))
              allocated = allocated.add(request.resource)
              resourcesSnapShot.update(request.workerId, (worker, availableResource.subtract(request.resource)))
            }
          } else {
            scheduleLater = scheduleLater :+ PendingRequest(appMaster, request, timeStamp)
          }
      }
    }
    for(request <- scheduleLater)
      resourceRequests.enqueue(request)
  }

  def resourceRequestHandler: Receive = {
    case RequestResource(appId, request) =>
      LOG.info(s"Request resource: appId: $appId, slots: ${request.resource.slots}")
      val appMaster = sender()
      resourceRequests.enqueue(new PendingRequest(appMaster, request, System.currentTimeMillis()))
      allocateResource()
  }

  private def allocateFairly(resources : mutable.HashMap[Int, (ActorRef, Resource)], pendindRequest : PendingRequest): Resource ={
    val length = resources.size
    val flattenResource = resources.toArray.zipWithIndex.flatMap((workerWithIndex) => {
      val ((workerId, (worker, resource)), index) = workerWithIndex
      0.until(resource.slots).map((seq) => ((workerId, worker), seq * length + index))
    }).sortBy(_._2).map(_._1)
    val PendingRequest(appMaster, request, timeStamp) = pendindRequest
    val total = Resource(flattenResource.size)

    val newAllocated = Resource.min(total, request.resource)
    val singleAllocation = flattenResource.take(newAllocated.slots)
      .groupBy((actor) => actor).mapValues(_.length).toArray.map((params) => {
      val ((workerId, worker), slots) = params
      resources.update(workerId, (worker, resources.get(workerId).get._2.subtract(Resource(slots))))
      ResourceAllocation(Resource(slots), worker, workerId)
    })
    pendindRequest.appMaster ! ResourceAllocated(singleAllocation)
    if (pendindRequest.request.resource.greaterThan(newAllocated)) {
      resourceRequests.enqueue(PendingRequest(appMaster, ResourceRequest(request.resource.subtract(newAllocated), request.workerId, request.priority), timeStamp))
    }
    newAllocated
  }

}
