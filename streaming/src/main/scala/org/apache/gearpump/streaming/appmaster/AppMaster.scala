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

package org.apache.gearpump.streaming.appmaster

import akka.actor._
import org.apache.gearpump._
import org.apache.gearpump.cluster.AppMasterToMaster.{WorkerData, GetWorkerData, GetAllWorkers}
import org.apache.gearpump.cluster.ClientToMaster.{QueryHistoryMetrics, ShutdownApplication}
import org.apache.gearpump.cluster.MasterToAppMaster.{WorkerList, AppMasterDataDetailRequest, AppMasterMetricsRequest, ReplayFromTimestampWindowTrailingEdge}
import org.apache.gearpump.cluster._
import org.apache.gearpump.metrics.Metrics.MetricType
import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.streaming.ExecutorToAppMaster._
import org.apache.gearpump.streaming._
import org.apache.gearpump.streaming.appmaster.AppMaster.AllocateResourceTimeOut
import org.apache.gearpump.streaming.appmaster.ExecutorManager.GetExecutorPathList
import org.apache.gearpump.streaming.appmaster.HistoryMetricsService.HistoryMetricsConfig
import org.apache.gearpump.streaming.appmaster.TaskManager.DagInit
import org.apache.gearpump.streaming.storage.InMemoryAppStoreOnMaster
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.util._
import org.slf4j.Logger

import scala.concurrent.Future

class AppMaster(appContext : AppMasterContext, app : Application) extends ApplicationMaster {
  import app.userConfig
  import appContext.{appId, masterProxy, username}
  implicit val dispatcher = context.dispatcher
  implicit val actorSystem = context.system
  implicit val timeOut = Constants.FUTURE_TIMEOUT

  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)
  LOG.info(s"AppMaster[$appId] is launched by $username $app xxxxxxxxxxxxxxxxx")
  private val address = ActorUtil.getFullPath(context.system, self.path)
  private var subscribers = false

  val dag = DAG(userConfig.getValue[Graph[TaskDescription, Partitioner]](AppDescription.DAG).get)

  private val (taskManager, executorManager, clockService) = {
    val executorManager = context.actorOf(ExecutorManager.props(userConfig, appContext))

    val store = new InMemoryAppStoreOnMaster(appId, appContext.masterProxy)
    val clockService = context.actorOf(Props(new ClockService(dag, store)))

    val taskScheduler: TaskScheduler = new TaskSchedulerImpl(appId)
    val taskManager = context.actorOf(Props(new TaskManager(appContext.appId, dag,
      taskScheduler, executorManager, clockService, self, app.name)))
    ipToWorkerIDMap().map{ ipToWorkerID =>
      taskScheduler.updateTaskSchedulePolicy(
        new ScheduleUsingDataDetector(context.system.settings.config, userConfig, ipToWorkerID, actorSystem))
      taskManager ! DagInit(dag)
    }
    (taskManager, executorManager, clockService)
  }

  private def getHistoryMetricsConfig: HistoryMetricsConfig = {
    val historyHour = context.system.settings.config.getInt(Constants.GEARPUMP_METRIC_RETAIN_HISTORY_DATA_HOURS)
    val historyInterval = context.system.settings.config.getInt(Constants.GEARPUMP_RETAIN_HISTORY_DATA_INTERVAL_MS)

    val recentSeconds = context.system.settings.config.getInt(Constants.GEARPUMP_RETAIN_RECENT_DATA_SECONDS)
    val recentInterval = context.system.settings.config.getInt(Constants.GEARPUMP_RETAIN_RECENT_DATA_INTERVAL_MS)
    HistoryMetricsConfig(historyHour, historyInterval, recentSeconds, recentInterval)
  }

  private val historyMetricsService = {
    context.actorOf(Props(new HistoryMetricsService(appId, getHistoryMetricsConfig)))
  }
  actorSystem.eventStream.subscribe(historyMetricsService, classOf[MetricType])

  override def receive : Receive =
    taskMessageHandler orElse
      executorMessageHandler orElse
      recover orElse
      appMasterInfoService orElse
      ActorUtil.defaultMsgHandler(self)

  def taskMessageHandler: Receive = {
    case clock: UpdateClock =>
      taskManager forward clock
    case GetLatestMinClock =>
      taskManager forward GetLatestMinClock
    case register: RegisterTask =>
      taskManager forward register
    case ReplayFromTimestampWindowTrailingEdge =>
      taskManager forward ReplayFromTimestampWindowTrailingEdge
    case metrics: MetricType =>
      if(subscribers) {
        LOG.info(s"***AppMaster publishing metrics***")
        actorSystem.eventStream.publish(metrics)
      }
  }

  def executorMessageHandler: Receive = {
    case register: RegisterExecutor =>
      executorManager forward register
  }

  def appMasterInfoService: Receive = {
    case appMasterDataDetailRequest: AppMasterDataDetailRequest =>
      LOG.info(s"***AppMaster got AppMasterDataDetailRequest for $appId ***")

      val executorsFuture = getExecutorList
      val clockFuture = getMinClock

      val appMasterDataDetail = for {executors <- executorsFuture
        clock <- clockFuture } yield {
        StreamingAppMasterDataDetail(appId, app.name, dag.processors,
          Graph.vertexHierarchyLevelMap(dag.graph), dag.graph, address, clock, executors)
      }

      val client = sender()

      appMasterDataDetail.map{appData =>
        client ! appData
      }
    case appMasterMetricsRequest: AppMasterMetricsRequest =>
      val client = sender()
      subscribers = true
      actorSystem.eventStream.subscribe(client, classOf[MetricType])

    case query: QueryHistoryMetrics =>
      historyMetricsService forward query
  }

  def recover: Receive = {
    case AllocateResourceTimeOut =>
      LOG.error(s"Failed to allocate resource in time")
      masterProxy ! ShutdownApplication(appId)
      context.stop(self)

  }

  import akka.pattern.ask
  private def getMinClock: Future[TimeStamp] = {
    (clockService ? GetLatestMinClock).asInstanceOf[Future[LatestMinClock]].map(_.clock)
  }

  private def getExecutorList: Future[List[String]] = {
    (executorManager ? GetExecutorPathList).asInstanceOf[Future[List[ActorPath]]].map(list => list.map(_.toString))
  }

  //Todo: Simplify this function, now the Master side could not provide direct ip info of workers
  private def ipToWorkerIDMap(): Future[Map[String, Set[Int]]] = {
    masterProxy.ask(GetAllWorkers).asInstanceOf[Future[WorkerList]].flatMap { workerList =>
      Future.fold(workerList.workers.map(masterProxy ? GetWorkerData(_)))(Map.empty[String, Set[Int]]){ (ipToWorkerIds, workerData) =>
        val description = workerData.asInstanceOf[WorkerData].workerDescription
        if(description.isEmpty || Util.parseIp(description.get.actorPath).isEmpty) {
          ipToWorkerIds
        } else {
          val ip = Util.parseIp(description.get.actorPath).get
          val workerIdSet = ipToWorkerIds.getOrElse(ip, Set.empty[Int])
          ipToWorkerIds + (ip -> (workerIdSet + description.get.workerId))
        }
      }
    }
  }
}

object AppMaster {
  case object AllocateResourceTimeOut
}