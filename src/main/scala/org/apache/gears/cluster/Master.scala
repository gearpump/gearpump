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

package org.apache.gears.cluster

import akka.actor._
import akka.remote.RemoteScope
import org.apache.gearpump._
import org.apache.gearpump.kvservice.SimpleKVService
import org.apache.gearpump.util.ActorSystemBooter.{BindLifeCycle, RegisterActorSystem}
import org.apache.gears.cluster.AppMasterToMaster._
import org.apache.gears.cluster.ClientToMaster._
import org.apache.gears.cluster.MasterToAppMaster._
import org.apache.gears.cluster.MasterToWorker._
import org.apache.gears.cluster.WorkerToMaster._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class Master extends Actor {
  import org.apache.gears.cluster.Master._

  private var resources = new Array[(ActorRef, Int)](0)
  private val resourceRequests = new mutable.Queue[(ActorRef, Int)]

  private var appManager : ActorRef = null
  private var workerId = 0

  override def receive : Receive = workerMsgHandler orElse appMasterMsgHandler orElse clientMsgHandler orElse terminationWatch orElse ActorUtil.defaultMsgHandler(self)

  def workerMsgHandler : Receive = {
    //create worker
    case RegisterActorSystem(systemPath) =>
      LOG.info(s"Received RegisterActorSystem $systemPath")
      val systemAddress = AddressFromURIString(systemPath)
      val workerConfig = Props(classOf[Worker], workerId, self).withDeploy(Deploy(scope = RemoteScope(systemAddress)))
      val worker = context.actorOf(workerConfig, classOf[Worker].getSimpleName + workerId)
      workerId += 1
      sender ! BindLifeCycle(worker)
    case RegisterWorker(id) =>
      context.watch(sender())
      sender ! WorkerRegistered
      LOG.info(s"Register Worker $id....")
    case ResourceUpdate(id, slots) =>
      LOG.info(s"Resource update id: $id, slots: $slots....")
      val current = sender()
      val index = resources.indexWhere((worker) => worker._1.equals(current), 0)
      if (index == -1) {
        resources = resources :+ (current, slots)
      } else {
        resources(index) = (current, slots)
      }
      allocateResource()
  }

  def appMasterMsgHandler : Receive = {
    case RequestResource(appId, slots) =>
      LOG.info(s"Request resource: appId: $appId, slots: $slots")
      val appMaster = sender()
      resourceRequests.enqueue((appMaster, slots))
      allocateResource()
  }

  def clientMsgHandler : Receive = {
    case app : SubmitApplication =>
      LOG.info(s"Receive from client, SubmitApplication $app")
      appManager.forward(app)
    case app : ShutdownApplication =>
      LOG.info(s"Receive from client, Shutting down Application ${app.appId}")
      appManager.forward(app)
    case ShutdownMaster =>
      LOG.info(s"Shutting down Master called from ${sender().toString()}...")
      context.stop(self)
  }

  def terminationWatch : Receive = {
    case t : Terminated =>
      val actor = t.actor
      LOG.info(s"worker ${actor.path} get terminated, is it due to network reason? ${t.getAddressTerminated()}")

      LOG.info("Let's filter out dead resources...")

      // filter out dead worker resource
      resources = resources.filter { resource =>
        val (worker, _) = resource
        worker.compareTo(actor) != 0
      }
  }

  // shutdown the hosting actor system
  override def postStop(): Unit = context.system.shutdown()

  def allocateResource(): Unit = {
    val length = resources.length
    val flattenResource = resources.zipWithIndex.flatMap((workerWithIndex) => {
      val ((worker, slots), index) = workerWithIndex
      0.until(slots).map((seq) => (worker, seq * length + index))
    }).sortBy(_._2).map(_._1)

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
    LOG.info(s"master path is $path")
    appManager = context.actorOf(Props[AppManager], classOf[AppManager].getSimpleName)
  }
}

object Master extends App with Starter {
  private val LOG: Logger = LoggerFactory.getLogger(Master.getClass)

  def uuid = java.util.UUID.randomUUID.toString

  def usage = List("The Master node must be started first",
    "java org.apache.gears.cluster.Master -port <port>")

  def start() = {
    val config = parse(args.toList)
    Console.println(s"Configuration after parse $config")
    validate(config)
    master(config.port)
  }

  def validate(config: Config): Unit = {
    if(config.port == -1) {
      commandHelp()
      System.exit(-1)
    }
  }

  def master(port : Int): Unit = {
    val url = s"http://127.0.0.1:$port/kv"
    Console.out.println(s"kv service url: $url")

    SimpleKVService.create.start(port)

    SimpleKVService.init(url)
    val system = ActorSystem("cluster", Configs.SYSTEM_DEFAULT_CONFIG)

    system.actorOf(Props[Master], "master")
    LOG.info("master is started...")

    val masterPath = ActorUtil.getSystemPath(system) + "/user/master"
    SimpleKVService.set("master", masterPath)
    system.awaitTermination()
  }

  start()

}


