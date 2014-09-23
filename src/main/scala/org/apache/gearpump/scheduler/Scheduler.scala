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

import akka.actor.{ActorRef, Actor}
import org.apache.gearpump.cluster.AppMasterToMaster.RequestResource
import org.apache.gearpump.cluster.Master.WorkerTerminated
import org.apache.gearpump.cluster.MasterToWorker.{UpdateResourceFailed, WorkerRegistered}
import org.apache.gearpump.cluster.WorkerToMaster.ResourceUpdate
import org.slf4j.{LoggerFactory, Logger}

import scala.collection.mutable

abstract class Scheduler extends Actor{
  private val LOG: Logger = LoggerFactory.getLogger(classOf[Scheduler])
  protected var resources = new mutable.HashMap[ActorRef, Resource]
  protected val resourceRequests = new mutable.Queue[(ActorRef, ResourceRequest)]

  def handleScheduleMessage : Receive = {
    case WorkerRegistered(id) =>
      val worker = sender()
      if(!resources.contains(worker)) {
        LOG.info(s"Worker $id added to the scheduler")
        resources.put(worker, Resource.empty)
      }
    case ResourceUpdate(id, resource) =>
      LOG.info(s"Resource update id: $id, slots: ${resource.slots}....")
      val current = sender()
      if(resources.contains(current)) {
        resources.update(current, resource)
        allocateResource()
      }
      else {
        current ! UpdateResourceFailed(s"ResourceUpdate failed! The worker $id has not been registered into master")
      }
    case RequestResource(appId, request)=>
      LOG.info(s"Request resource: appId: $appId, slots: ${request.resource.slots}")
      val appMaster = sender()
      resourceRequests.enqueue((appMaster, request))
      allocateResource()
    case WorkerTerminated(actor) =>
      if(resources.contains(actor)){
        resources -= actor
      }
  }

  def allocateResource() : Unit
}
