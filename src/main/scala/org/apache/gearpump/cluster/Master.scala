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

package org.apache.gearpump.cluster

import java.util.concurrent.TimeUnit

import akka.actor._
import com.typesafe.config.Config
import org.apache.gearpump.cluster.AppMasterToMaster._
import org.apache.gearpump.cluster.ClientToMaster._
import org.apache.gearpump.cluster.MasterToAppMaster._
import org.apache.gearpump.cluster.MasterToScheduler.WorkerTerminated
import org.apache.gearpump.cluster.MasterToWorker._
import org.apache.gearpump.cluster.WorkerToMaster._
import org.apache.gearpump.util.ActorSystemBooter.{BindLifeCycle, RegisterActorSystem}
import org.apache.gearpump.util.{Constants, ActorUtil}
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.forkjoin.ThreadLocalRandom

private[cluster] class Master extends Actor with Stash {

  private val LOG: Logger = LoggerFactory.getLogger(classOf[Master])
  private val systemConfig : Config = context.system.settings.config
  // resources and resourceRequests can be dynamically constructed by
  // heartbeat of worker and appmaster when master singleton is migrated.
  // we don't need to persist them in cluster

  private var appManager : ActorRef = null

  private var scheduler : ActorRef = null

  private var workers = new immutable.HashMap[ActorRef, Int]

  LOG.info("master is started at " + ActorUtil.getFullPath(context) + "...")

  override def receive : Receive = workerMsgHandler orElse appMasterMsgHandler orElse clientMsgHandler orElse terminationWatch orElse ActorUtil.defaultMsgHandler(self)

  final val undefinedUid = 0
  @tailrec final def newUid(): Int = {
    val uid = ThreadLocalRandom.current.nextInt()
    if (uid == undefinedUid) newUid
    else uid
  }

  def workerMsgHandler : Receive = {
    case RegisterNewWorker =>
      val workerId = newUid
      self forward RegisterWorker(workerId)
    case RegisterWorker(id) =>
      context.watch(sender())
      sender ! WorkerRegistered(id)
      scheduler forward WorkerRegistered(id)
      workers += (sender() -> id)
      LOG.info(s"Register Worker $id....")
    case resourceUpdate : ResourceUpdate =>
      scheduler forward resourceUpdate
  }

  def appMasterMsgHandler : Receive = {
    case  request : RequestResource =>
      scheduler forward request
    case registerAppMaster : RegisterAppMaster =>
      //forward to appmaster
      appManager forward registerAppMaster
  }

  def clientMsgHandler : Receive = {
    case app : SubmitApplication =>
      LOG.info(s"Receive from client, SubmitApplication $app")
      appManager.forward(app)
    case app : ShutdownApplication =>
      LOG.info(s"Receive from client, Shutting down Application ${app.appId}")
      appManager.forward(app)
  }

  def terminationWatch : Receive = {
    case t : Terminated =>
      val actor = t.actor
      LOG.info(s"worker ${actor.path} get terminated, is it due to network reason? ${t.getAddressTerminated()}")
      LOG.info("Let's filter out dead resources...")
      // filter out dead worker resource
      if(workers.keySet.contains(actor)){
        scheduler ! WorkerTerminated(actor)
        workers -= actor
      }
  }

  override def preStart(): Unit = {
    val path = ActorUtil.getFullPath(context)
    LOG.info(s"master path is $path")
    val schedulerClass = Class.forName(systemConfig.getString(Constants.GEARPUMP_SCHEDULER))
    appManager = context.actorOf(Props[AppManager], classOf[AppManager].getSimpleName)
    scheduler = context.actorOf(Props(schedulerClass))
  }
}