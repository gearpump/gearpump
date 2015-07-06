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

package org.apache.gearpump.cluster.master

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern.ask
import akka.remote.DisassociatedEvent
import akka.util.Timeout
import com.typesafe.config.Config
import org.apache.gearpump.cluster.AppMasterToMaster._
import org.apache.gearpump.cluster.ClientToMaster._
import org.apache.gearpump.cluster.MasterToAppMaster._
import org.apache.gearpump.cluster.MasterToClient.MasterConfig
import org.apache.gearpump.cluster.MasterToWorker._
import org.apache.gearpump.cluster.WorkerToMaster._
import org.apache.gearpump.cluster.master.Master.{MasterInfo, WorkerTerminated, _}
import org.apache.gearpump.cluster.scheduler.Scheduler.ApplicationFinished
import org.apache.gearpump.jarstore.{JarFileContainerWrapper, JarFileContainer, JarStore}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util._
import org.slf4j.Logger

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.{Await, Future}

private[cluster] class Master extends Actor with Stash {
  import context.dispatcher
  private val LOG: Logger = LogUtil.getLogger(getClass)
  private val systemConfig : Config = context.system.settings.config
  private implicit val timeout = Constants.FUTURE_TIMEOUT
  // resources and resourceRequests can be dynamically constructed by
  // heartbeat of worker and appmaster when master singleton is migrated.
  // we don't need to persist them in cluster

  private var appManager : ActorRef = null

  private var scheduler : ActorRef = null

  private var workers = new immutable.HashMap[ActorRef, Int]

  private val birth = System.currentTimeMillis()

  LOG.info("master is started at " + ActorUtil.getFullPath(context.system, self.path) + "...")

  val jarStoreRootPath = systemConfig.getString(Constants.GEARPUMP_APP_JAR_STORE_ROOT_PATH)
  val jarStore = context.actorOf(JarStore.props(jarStoreRootPath))

  private val hostPort = HostPort(ActorUtil.getSystemAddress(context.system).hostPort)

  override def receive : Receive = workerMsgHandler orElse
    appMasterMsgHandler orElse
    clientMsgHandler orElse
    jarStoreService orElse
    terminationWatch orElse
    disassociated orElse
    ActorUtil.defaultMsgHandler(self)

  final val undefinedUid = 0
  @tailrec final def newUid(): Int = {
    val uid = Util.randInt
    if (uid == undefinedUid) newUid()
    else uid
  }

  def workerMsgHandler : Receive = {
    case RegisterNewWorker =>
      val workerId = newUid()
      self forward RegisterWorker(workerId)
    case RegisterWorker(id) =>
      context.watch(sender())
      sender ! WorkerRegistered(id, MasterInfo(self, birth))
      scheduler forward WorkerRegistered(id, MasterInfo(self, birth))
      workers += (sender() -> id)
      val workerHostname = ActorUtil.getHostname(sender())
      LOG.info(s"Register Worker $id from $workerHostname ....")
    case resourceUpdate : ResourceUpdate =>
      scheduler forward resourceUpdate
  }

  def jarStoreService : Receive = {
    case GetJarFileContainer =>
      val client = sender
      (jarStore ? GetJarFileContainer).asInstanceOf[Future[JarFileContainer]].map{container =>
        client ! new JarFileContainerWrapper(container)
      }
  }

  def appMasterMsgHandler : Receive = {
    case  request : RequestResource =>
      scheduler forward request
    case registerAppMaster : RegisterAppMaster =>
      //forward to appManager
      appManager forward registerAppMaster
    case save : SaveAppData =>
      appManager forward save
    case get : GetAppData =>
      appManager forward get
    case GetAllWorkers =>
      sender ! WorkerList(workers.values.toList)
    case get@GetWorkerData(workerId) =>
      workers.find(_._2 == workerId) match {
        case Some((ref, _)) =>
          ref forward get
        case None =>
          sender ! WorkerData(None)
      }

    case GetMasterData =>
      val aliveFor = System.currentTimeMillis() - birth
      val logFileDir = LogUtil.daemonLogDir(systemConfig).getAbsolutePath
      val userDir = System.getProperty("user.dir");
      val masterDescription = MasterDescription(hostPort.toTuple, getMasterClusterList.map(_.toTuple), aliveFor, logFileDir, jarStoreRootPath, MasterStatus.Synced, userDir)
      sender ! MasterData(masterDescription)

    case invalidAppMaster: InvalidAppMaster =>
      appManager forward invalidAppMaster
  }

  private def getMasterClusterList: List[HostPort] = {
    val cluster = systemConfig.getStringList(GEARPUMP_CLUSTER_MASTERS)
      .asScala.map(HostPort(_)).toList

    if (cluster.isEmpty) {

      //add myself into the list if it is a single node cluster
      List(hostPort)
    } else {
      cluster
    }
  }

  def clientMsgHandler : Receive = {
    case app : SubmitApplication =>
      LOG.debug(s"Receive from client, SubmitApplication $app")
      appManager.forward(app)
    case app : RestartApplication =>
      LOG.debug(s"Receive from client, RestartApplication $app")
      appManager.forward(app)
    case app : ShutdownApplication =>
      LOG.debug(s"Receive from client, Shutting down Application ${app.appId}")
      scheduler ! ApplicationFinished(app.appId)
      appManager.forward(app)
    case app : ReplayFromTimestampWindowTrailingEdge =>
      LOG.debug(s"Receive from client, Replaying Application ${app.appId} from timestamp window trailing edge")
      appManager.forward(app)
    case app : ResolveAppId =>
      LOG.debug(s"Receive from client, resolving appId ${app.appId} to ActorRef")
      appManager.forward(app)
    case app : AppMasterDataDetailRequest =>
      LOG.debug(s"Receive from client, forwarding to AppManager")
      appManager.forward(app)
    case app : AppMasterMetricsRequest =>
      LOG.debug(s"AppMasterMetricsRequestFromActor Receive from client, forwarding to AppManager")
      appManager.forward(app)

    case AppMastersDataRequest =>
      LOG.debug("Master received AppMastersDataRequest")
      appManager forward AppMastersDataRequest
    case appMasterDataRequest: AppMasterDataRequest =>
      LOG.debug("Master received AppMasterDataRequest")
      appManager forward appMasterDataRequest
    case query: QueryAppMasterConfig =>
      LOG.debug("Master received QueryAppMasterConfig")
      appManager forward query
    case QueryMasterConfig =>
      sender ! MasterConfig(systemConfig)
    case query: QueryHistoryMetrics =>
      LOG.debug("Master received QueryHistoryMetrics")
      appManager forward query
    case getstalling: GetStallingTasks =>
      LOG.debug("Master received GetStallingTasks")
      appManager forward getstalling
  }

  def disassociated : Receive = {
    case disassociated : DisassociatedEvent =>
      LOG.info(s" disassociated ${disassociated.remoteAddress}")
      //LOG.info(s"remote lifecycle events are "+systemConfig.getString("akka.remote.log-remote-lifecycle-events"))
  }

  def terminationWatch : Receive = {
    case t : Terminated =>
      val actor = t.actor
      LOG.info(s"worker ${actor.path} get terminated, is it due to network reason? ${t.getAddressTerminated()}")
      LOG.info("Let's filter out dead resources...")
      // filter out dead worker resource
      if(workers.keySet.contains(actor)){
        scheduler ! WorkerTerminated(workers.get(actor).get)
        workers -= actor
      }
  }

  override def preStart(): Unit = {
    val path = ActorUtil.getFullPath(context.system, self.path)
    LOG.info(s"master path is $path")
    val schedulerClass = Class.forName(systemConfig.getString(Constants.GEARPUMP_SCHEDULING_SCHEDULER))

    val masterHA = context.actorOf(Props(new MasterHAService()), "masterHA")
    val kvService = context.actorOf(Props(new InMemoryKVService()), "kvService")

    appManager = context.actorOf(Props(new AppManager(masterHA, kvService, AppMasterLauncher)), classOf[AppManager].getSimpleName)
    scheduler = context.actorOf(Props(schedulerClass))
    context.system.eventStream.subscribe(self, classOf[DisassociatedEvent])
  }
}

object Master {

  case class WorkerTerminated(workerId: Int)

  case class MasterInfo(master: ActorRef, startTime : Long = 0L)

  object MasterInfo {
    def empty = MasterInfo(null)
  }

  case class SlotStatus(totalSlots: Int, availableSlots: Int)
}