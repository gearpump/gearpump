package org.apache.gearpump
/**
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
import akka.actor._
import org.apache.gearpump.service.SimpleKVService
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.Queue

object Master {
  private val LOG: Logger = LoggerFactory.getLogger(Master.getClass)

  def main(argStrings: Array[String]) {

    val kvService = argStrings(0);
    SimpleKVService.init(kvService)

    val system = ActorSystem("master", Configs.SYSTEM_DEFAULT_CONFIG)

    LOG.info("master process is started...");

    val actor = system.actorOf(Props[Master], "master")

    system.awaitTermination()
    LOG.info("master process is shutdown...");
  }
}

class Master extends Actor {
  import org.apache.gearpump.Master._

  private var resources = new Array[(ActorRef, Int)](0)
  private val resourceRequests = new Queue[(ActorRef, Int)]

  override def receive : Receive = workerMsgHandler orElse  appMasterMsgHandler orElse  ActorUtil.defaultMsgHandler

  def workerMsgHandler : Receive = {
    case RegisterWorker =>
      sender ! WorkerRegistered
      LOG.info("Register Worker ....")
    case ResourceUpdate(slots) =>
      LOG.info("Resoource update ...." + slots)
      val current = sender;
      val index = resources.indexWhere((worker) => worker._1.equals(current), 0)
      if (index == -1) {
        resources = resources :+ (current, slots)
      } else {
        resources(index) = (current, slots)
      }
      allocateResource
  }

  def appMasterMsgHandler : Receive = {
    case RequestResource(slots) =>
      LOG.info("Request resource: " + slots)
      val appMaster = sender
      resourceRequests.enqueue((appMaster, slots))
      allocateResource
  }

  def allocateResource : Unit = {
    val length = resources.length
    val flattenResource = resources.zipWithIndex.flatMap((workerWithIndex) => {
      val ((worker, slots), index) = workerWithIndex
      0.until(slots).map((seq) => (worker, seq * length + index))
    }).asInstanceOf[Array[(ActorRef, Int)]].sortBy(_._2).map(_._1)

    val total = flattenResource.length
    def assignResourceToApplication(allocated : Int) : Unit = {
      if (allocated == total || resourceRequests.isEmpty) {
        return
      }

      val (appMaster, slots) = resourceRequests.dequeue()
      val newAllocated = Math.min(total - allocated, slots)
      val singleAllocation = flattenResource.slice(allocated, allocated + newAllocated)
        .groupBy((actor) => actor).mapValues(_.length).toArray.map((resource) => Resource(resource._1, resource._2))
      appMaster ! ResourceAllocated(singleAllocation)
      if (slots > newAllocated) {
        resourceRequests.enqueue((appMaster, slots - newAllocated))
      }
      assignResourceToApplication(allocated + newAllocated)
    }

    assignResourceToApplication(0)
  }

  override def preStart(): Unit = {
    val path = ActorUtil.getFullPath(context)
    SimpleKVService.set("master", path)
    LOG.info("master path is: " +  path)
  }
}


