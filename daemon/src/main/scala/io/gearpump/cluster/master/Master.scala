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

package io.gearpump.cluster.master

import java.lang.management.ManagementFactory

import akka.actor._
import akka.remote.DisassociatedEvent
import com.typesafe.config.Config
import io.gearpump.cluster.AppMasterToMaster._
import io.gearpump.cluster.ClientToMaster._
import io.gearpump.cluster.MasterToAppMaster._
import io.gearpump.cluster.MasterToClient.{ResolveWorkerIdResult, MasterConfig}
import io.gearpump.cluster.MasterToWorker._
import io.gearpump.cluster.WorkerToMaster._
import io.gearpump.cluster.master.InMemoryKVService.{GetKV, GetKVFailed, GetKVSuccess, PutKV}
import io.gearpump.cluster.master.Master.{MasterInfo, WorkerTerminated, _}
import io.gearpump.cluster.scheduler.Scheduler.ApplicationFinished
import io.gearpump.metrics.Metrics.ReportMetrics
import io.gearpump.metrics.{MetricsReporterService, Metrics, JvmMetricsSet}
import io.gearpump.jarstore.local.LocalJarStore
import io.gearpump.transport.HostPort
import io.gearpump.util.Constants._
import io.gearpump.util.HistoryMetricsService.HistoryMetricsConfig
import io.gearpump.util._
import org.slf4j.Logger

import scala.collection.JavaConverters._
import scala.collection.immutable

private[cluster] class Master extends Actor with Stash {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  private val systemConfig : Config = context.system.settings.config
  private implicit val timeout = Constants.FUTURE_TIMEOUT
  private val kvService = context.actorOf(Props(new InMemoryKVService()), "kvService")
  // resources and resourceRequests can be dynamically constructed by
  // heartbeat of worker and appmaster when master singleton is migrated.
  // we don't need to persist them in cluster

  private var appManager : ActorRef = null

  private var scheduler : ActorRef = null

  private var workers = new immutable.HashMap[ActorRef, Int]

  private val birth = System.currentTimeMillis()

  private var nextWorkerId = 0

  def receive: Receive = null

  // register jvm metrics
  Metrics(context.system).register(new JvmMetricsSet(s"master"))

  LOG.info("master is started at " + ActorUtil.getFullPath(context.system, self.path) + "...")

  val jarStoreRootPath = systemConfig.getString(Constants.GEARPUMP_APP_JAR_STORE_ROOT_PATH)

  val jarStore = if(Util.isLocalPath(jarStoreRootPath)) {
    Some(context.actorOf(Props(classOf[LocalJarStore], jarStoreRootPath)))
  } else{
    None
  }

  private val hostPort = HostPort(ActorUtil.getSystemAddress(context.system).hostPort)

  val metricsEnabled = systemConfig.getBoolean(GEARPUMP_METRIC_ENABLED)
  val historyMetricsService = if (metricsEnabled) {
    val getHistoryMetricsConfig = HistoryMetricsConfig(systemConfig)
    val historyMetricsService = {
      context.actorOf(Props(new HistoryMetricsService("master", getHistoryMetricsConfig)))
    }

    val metricsReportService = context.actorOf(Props(new MetricsReporterService(Metrics(context.system))))
    historyMetricsService.tell(ReportMetrics, metricsReportService)
    Some(historyMetricsService)
  } else {
    None
  }

  kvService ! GetKV(MASTER_GROUP, WORKER_ID)
  context.become(waitForNextWorkerId)

  def waitForNextWorkerId: Receive = {
    case GetKVSuccess(_, result) =>
      if(result != null) {
        this.nextWorkerId = result.asInstanceOf[Int]
      }
      context.become(receiveHandler)
      unstashAll()
    case GetKVFailed(ex) =>
      LOG.error("Failed to get worker id, shutting down master to avoid data corruption...")
      context.parent ! PoisonPill
    case msg =>
      LOG.info(s"Get message ${msg.getClass.getSimpleName}")
      stash()
  }

  def receiveHandler : Receive = workerMsgHandler orElse
    appMasterMsgHandler orElse
    clientMsgHandler orElse
    metricsService orElse
    jarStoreService orElse
    terminationWatch orElse
    disassociated orElse
    ActorUtil.defaultMsgHandler(self)

  def workerMsgHandler : Receive = {
    case RegisterNewWorker =>
      val workerId = nextWorkerId
      nextWorkerId += 1
      kvService ! PutKV(MASTER_GROUP, WORKER_ID, nextWorkerId)
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
    case GetJarStoreServer =>
      jarStore.foreach(_ forward GetJarStoreServer)
  }

  def metricsService : Receive = {
    case query: QueryHistoryMetrics =>
      historyMetricsService.foreach(_ forward query)
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
    case GetMasterData =>
      val aliveFor = System.currentTimeMillis() - birth
      val logFileDir = LogUtil.daemonLogDir(systemConfig).getAbsolutePath
      val userDir = System.getProperty("user.dir");

      val masterDescription =
        MasterSummary(
          hostPort.toTuple,
          getMasterClusterList.map(_.toTuple),
          aliveFor,
          logFileDir,
          jarStoreRootPath,
          MasterStatus.Synced,
          userDir,
          List.empty[MasterActivity],
          jvmName = ManagementFactory.getRuntimeMXBean().getName())

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

  import scala.util.{Success, Failure}

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
    case app : ResolveAppId =>
      LOG.debug(s"Receive from client, resolving appId ${app.appId} to ActorRef")
      appManager.forward(app)
    case resolve: ResolveWorkerId =>
      LOG.debug(s"Receive from client, resolving workerId ${resolve.workerId}")
      val worker = workers.find(_._2 == resolve.workerId)
      worker match {
        case Some(worker) => sender ! ResolveWorkerIdResult(Success(worker._1))
        case None => sender ! ResolveWorkerIdResult(Failure(new Exception(s"cannot find worker ${resolve.workerId}")))
      }
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

    appManager = context.actorOf(Props(new AppManager(kvService, AppMasterLauncher)), classOf[AppManager].getSimpleName)
    scheduler = context.actorOf(Props(schedulerClass))
    context.system.eventStream.subscribe(self, classOf[DisassociatedEvent])
  }
}

object Master {
  final val MASTER_GROUP = "-1"

  final val WORKER_ID = "next_worker_id"

  case class WorkerTerminated(workerId: Int)

  case class MasterInfo(master: ActorRef, startTime : Long = 0L)

  object MasterInfo {
    def empty = MasterInfo(null)
  }

  case class SlotStatus(totalSlots: Int, availableSlots: Int)
}