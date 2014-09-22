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

import akka.actor.{Terminated, ActorRef, Actor}
import org.apache.gearpump.cluster.AppMasterToMaster.RequestResource
import org.apache.gearpump.cluster.WorkerToMaster.ResourceUpdate
import org.slf4j.{LoggerFactory, Logger}

import scala.collection.mutable

abstract class Scheduler extends Actor{
  private val LOG: Logger = LoggerFactory.getLogger(classOf[Scheduler])
  protected var resources = new Array[(ActorRef, Resource)](0)
  protected val resourceRequests = new mutable.Queue[(ActorRef, Allocation)]

  override def receive : Receive = handle

  def handle : Receive = {
    case ResourceUpdate(id, resource) =>
      val current = sender()
      val index = resources.indexWhere((worker) => worker._1.equals(current), 0)
      if (index == -1) {
        resources = resources :+ (current,resource)
      } else {
        resources(index) = (current, resource)
      }
      allocateResource()
    case RequestResource(appId, resource)=>
      val appMaster = sender()
      resourceRequests.enqueue((appMaster, resource))
      allocateResource()
    case t : Terminated =>
      val actor = t.actor
      resources = resources.filter{ resource =>
        val (worker, _) = resource
        worker.compareTo(actor) != 0
      }
  }

  def allocateResource() : Unit
}
