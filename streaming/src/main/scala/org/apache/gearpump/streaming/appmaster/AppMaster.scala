/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming.appmaster

import java.lang.management.ManagementFactory

import akka.actor._
import org.apache.gearpump._
import org.apache.gearpump.cluster.AppMasterToMaster.ApplicationStatusChanged
import org.apache.gearpump.cluster.ClientToMaster._
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterActivated, AppMasterDataDetailRequest, ReplayFromTimestampWindowTrailingEdge}
import org.apache.gearpump.cluster.MasterToClient.{HistoryMetrics, HistoryMetricsItem, LastFailure}
import org.apache.gearpump.cluster._
import org.apache.gearpump.cluster.worker.WorkerId
import org.apache.gearpump.metrics.Metrics.ReportMetrics
import org.apache.gearpump.metrics.{JvmMetricsSet, Metrics, MetricsReporterService}
import org.apache.gearpump.streaming.partitioner.PartitionerDescription
import org.apache.gearpump.streaming.ExecutorToAppMaster.{MessageLoss, RegisterExecutor, RegisterTask, UnRegisterTask}
import org.apache.gearpump.streaming._
import org.apache.gearpump.streaming.appmaster.AppMaster._
import org.apache.gearpump.streaming.appmaster.DagManager.{GetLatestDAG, LatestDAG, ReplaceProcessor}
import org.apache.gearpump.streaming.appmaster.ExecutorManager.{ExecutorInfo, GetExecutorInfo}
import org.apache.gearpump.streaming.appmaster.TaskManager.{ApplicationReady, FailedToRecover, GetTaskList, TaskList}
import org.apache.gearpump.streaming.executor.Executor.{ExecutorConfig, ExecutorSummary, GetExecutorSummary, QueryExecutorConfig}
import org.apache.gearpump.streaming.storage.InMemoryAppStoreOnMaster
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.streaming.util.ActorPathUtil
import org.apache.gearpump.util.Constants.{APPMASTER_DEFAULT_EXECUTOR_ID, _}
import org.apache.gearpump.util.HistoryMetricsService.HistoryMetricsConfig
import org.apache.gearpump.util._
import org.slf4j.Logger

import scala.concurrent.Future

/**
 * AppMaster is the head of a streaming application.
 *
 * It contains:
 *  1. ExecutorManager to manage all executors.
 *  2. TaskManager to manage all tasks,
 *  3. ClockService to track the global clock for this streaming application.
 *  4. Scheduler to decide which a task should be scheduled to.
 */
class AppMaster(appContext: AppMasterContext, app: AppDescription) extends ApplicationMaster {
  import app.userConfig
  import appContext.{appId, masterProxy, username}

  private implicit val actorSystem = context.system
  private implicit val timeOut = FUTURE_TIMEOUT

  import akka.pattern.ask
  private implicit val dispatcher = context.dispatcher

  private val startTime: TimeStamp = System.currentTimeMillis()

  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)
  LOG.info(s"AppMaster[$appId] is launched by $username, app: $app xxxxxxxxxxxxxxxxx")
  LOG.info(s"AppMaster actor path: ${ActorUtil.getFullPath(context.system, self.path)}")

  private val address = ActorUtil.getFullPath(context.system, self.path)

  private val store = new InMemoryAppStoreOnMaster(appId, appContext.masterProxy)
  private val dagManager = context.actorOf(Props(new DagManager(appContext.appId, userConfig, store,
    Some(getUpdatedDAG))))

  private var taskManager: Option[ActorRef] = None
  private var clockService: Option[ActorRef] = None
  private val systemConfig = context.system.settings.config
  private var lastFailure = LastFailure(0L, null)

  private val appMasterBrief = ExecutorBrief(APPMASTER_DEFAULT_EXECUTOR_ID,
    self.path.toString,
    Option(appContext.workerInfo).map(_.workerId).getOrElse(WorkerId.unspecified), "active")

  private val getHistoryMetricsConfig = HistoryMetricsConfig(systemConfig)

  private val metricsEnabled = systemConfig.getBoolean(GEARPUMP_METRIC_ENABLED)

  private val userDir = System.getProperty("user.dir")
  private val logFile = LogUtil.applicationLogDir(actorSystem.settings.config)

  private val appMasterExecutorSummary = ExecutorSummary(
    APPMASTER_DEFAULT_EXECUTOR_ID,
    Option(appContext.workerInfo).map(_.workerId).getOrElse(WorkerId.unspecified),
    self.path.toString,
    logFile.getAbsolutePath,
    status = "Active",
    taskCount = 0,
    tasks = Map.empty[ProcessorId, List[TaskId]],
    jvmName = ManagementFactory.getRuntimeMXBean.getName
  )

  private val historyMetricsService = if (metricsEnabled) {
    // Registers jvm metrics
    Metrics(context.system).register(new JvmMetricsSet(
      s"app$appId.executor$APPMASTER_DEFAULT_EXECUTOR_ID"))

    val historyMetricsService = context.actorOf(Props(new HistoryMetricsService(
      s"app$appId", getHistoryMetricsConfig)))

    val metricsReportService = context.actorOf(Props(
      new MetricsReporterService(Metrics(context.system))))
    historyMetricsService.tell(ReportMetrics, metricsReportService)

    Some(historyMetricsService)
  } else {
    None
  }

  private val executorManager: ActorRef =
    context.actorOf(ExecutorManager.props(userConfig, appContext, app.clusterConfig, app.name),
      ActorPathUtil.executorManagerActorName)

  for (dag <- getDAG) {
    clockService = Some(context.actorOf(Props(new ClockService(dag, store))))
    val jarScheduler = new JarScheduler(appId, app.name, systemConfig, context)

    taskManager = Some(context.actorOf(Props(new TaskManager(appContext.appId, dagManager,
      jarScheduler, executorManager, clockService.get, self, app.name))))
  }

  override def receive: Receive = {
    taskMessageHandler orElse
      executorMessageHandler orElse
      ready orElse
      recover orElse
      appMasterService orElse
      ActorUtil.defaultMsgHandler(self)
  }

  /** Handles messages from Tasks */
  def taskMessageHandler: Receive = {
    case clock: ClockEvent =>
      clockService.foreach(_ forward clock)
    case register: RegisterTask =>
      taskManager.foreach(_ forward register)
    case unRegister: UnRegisterTask =>
      taskManager.foreach(_ forward unRegister)
      // Checks whether this processor dead, if it is, then we should remove it from clockService.
      clockService.foreach(_ forward CheckProcessorDeath(unRegister.taskId.processorId))
    case replay: ReplayFromTimestampWindowTrailingEdge =>
      if (replay.appId == appId) {
        taskManager.foreach(_ forward replay)
      } else {
        LOG.error(s"replay for invalid appId ${replay.appId}")
      }
    case messageLoss: MessageLoss =>
      lastFailure = LastFailure(System.currentTimeMillis(), messageLoss.cause)
      taskManager.foreach(_ forward messageLoss)
    case lookupTask: LookupTaskActorRef =>
      taskManager.foreach(_ forward lookupTask)
    case GetDAG =>
      val task = sender()
      getDAG.foreach {
        dag => task ! dag
      }
  }

  /** Handles messages from Executors */
  def executorMessageHandler: Receive = {
    case register: RegisterExecutor =>
      executorManager forward register
    case ReportMetrics =>
      historyMetricsService.foreach(_ forward ReportMetrics)
  }

  /** Handles messages from AppMaster */
  def appMasterService: Receive = {
    case AppMasterDataDetailRequest(rid) =>
      if (rid == appId) {
        LOG.info(s"AppMaster got AppMasterDataDetailRequest for $appId ")

        val executorsFuture = executorBrief
        val clockFuture = getMinClock
        val taskFuture = getTaskList
        val dagFuture = getDAG

        val appMasterDataDetail = for {
          executors <- executorsFuture
          clock <- clockFuture
          tasks <- taskFuture
          dag <- dagFuture
        } yield {
          val graph = dag.graph

          val executorToTasks = tasks.tasks.groupBy(_._2).mapValues {
            _.keys.toList
          }

          val processors = dag.processors.map { kv =>
            val processor = kv._2
            import processor._
            val tasks = executorToTasks.map { kv =>
              (kv._1, TaskCount(kv._2.count(_.processorId == id)))
            }.filter(_._2.count != 0)
            (id, ProcessorSummary(id, taskClass, parallelism, description, taskConf, life,
              tasks.keys.toList, tasks))
          }

          StreamAppMasterSummary(
            appId = appId,
            appName = app.name,
            actorPath = address,
            clock = clock,
            status = ApplicationStatus.ACTIVE,
            startTime = startTime,
            uptime = System.currentTimeMillis() - startTime,
            user = username,
            homeDirectory = userDir,
            logFile = logFile.getAbsolutePath,
            processors = processors,
            processorLevels = graph.vertexHierarchyLevelMap(),
            dag = graph.mapEdge { (node1, edge, node2) =>
              edge.partitionerFactory.name
            },
            executors = executors,
            historyMetricsConfig = getHistoryMetricsConfig
          )
        }

        val client = sender()

        appMasterDataDetail.map { appData =>
          client ! appData
        }
      } else {
        LOG.error(s"AppMasterDataDetailRequest for invalid appId $rid")
      }
    // TODO: WebSocket is buggy and disabled.
    //    case appMasterMetricsRequest: AppMasterMetricsRequest =>
    //      val client = sender()
    //      actorSystem.eventStream.subscribe(client, classOf[MetricType])
    case query: QueryHistoryMetrics =>
      if (historyMetricsService.isEmpty) {
        // Returns empty metrics so that we don't hang the UI
        sender ! HistoryMetrics(query.path, List.empty[HistoryMetricsItem])
      } else {
        historyMetricsService.get forward query
      }
    case getStalling: GetStallingTasks =>
      clockService.foreach(_ forward getStalling)
    case replaceDAG: ReplaceProcessor =>
      dagManager forward replaceDAG
    case GetLastFailure(id) =>
      if (id == appId) {
        sender ! lastFailure
      } else {
        LOG.error(s"GetLastFailure for invalid appId $id")
      }
    case get@GetExecutorSummary(executorId) =>
      val client = sender()
      if (executorId == APPMASTER_DEFAULT_EXECUTOR_ID) {
        client ! appMasterExecutorSummary
      } else {
        ActorUtil.askActor[Map[ExecutorId, ExecutorInfo]](executorManager, GetExecutorInfo)
          .map { map =>
            map.get(executorId).foreach { executor =>
              executor.executor.tell(get, client)
            }
          }
      }
    case query@QueryExecutorConfig(executorId) =>
      val client = sender()
      if (executorId == -1) {
        val systemConfig = context.system.settings.config
        sender ! ExecutorConfig(ClusterConfig.filterOutDefaultConfig(systemConfig))
      } else {
        ActorUtil.askActor[Map[ExecutorId, ExecutorInfo]](executorManager, GetExecutorInfo)
          .map { map =>
            map.get(executorId).foreach { executor =>
              executor.executor.tell(query, client)
            }
          }
      }
  }

  def ready: Receive = {
    case ApplicationReady =>
      masterProxy ! ApplicationStatusChanged(appId, ApplicationStatus.ACTIVE,
        System.currentTimeMillis(), null)
    case AppMasterActivated(id) =>
      LOG.info(s"AppMaster for app$id is activated")
  }

  /** Error handling */
  def recover: Receive = {
    case FailedToRecover(errorMsg) =>
      if (context.children.toList.contains(sender())) {
        LOG.error(errorMsg)
        val failed = ApplicationStatusChanged(appId, ApplicationStatus.FAILED, lastFailure.time,
          new Exception(lastFailure.error))
        masterProxy ! failed
      }
    case AllocateResourceTimeOut =>
      val errorMsg = s"Failed to allocate resource in time, shutdown application $appId"
      LOG.error(errorMsg)
      val failed = ApplicationStatusChanged(appId, ApplicationStatus.FAILED,
        System.currentTimeMillis(), new Exception(lastFailure.error))
      masterProxy ! failed
      context.stop(self)
  }

  private def getMinClock: Future[TimeStamp] = {
    clockService match {
      case Some(service) =>
        (service ? GetLatestMinClock).asInstanceOf[Future[LatestMinClock]].map(_.clock)
      case None =>
        Future.failed(new ServiceNotAvailableException("clock service not ready"))
    }
  }

  private def executorBrief: Future[List[ExecutorBrief]] = {
    ActorUtil.askActor[Map[ExecutorId, ExecutorInfo]](executorManager, GetExecutorInfo)
      .map { infos =>
        infos.values.map { info =>
          ExecutorBrief(info.executorId,
            info.executor.path.toSerializationFormat,
            info.worker.workerId,
            "active")
        }.toList :+ appMasterBrief
      }
  }

  private def getTaskList: Future[TaskList] = {
    taskManager match {
      case Some(manager) =>
        (manager ? GetTaskList).asInstanceOf[Future[TaskList]]
      case None =>
        Future.failed(new ServiceNotAvailableException("task manager not ready"))
    }
  }

  private def getDAG: Future[DAG] = {
    (dagManager ? GetLatestDAG).asInstanceOf[Future[LatestDAG]].map(_.dag)
  }

  private def getUpdatedDAG: DAG = {
    val dag = DAG(userConfig.getValue[Graph[ProcessorDescription,
      PartitionerDescription]](StreamApplication.DAG).get)
    val updated = dag.processors.map { idAndProcessor =>
      val (id, oldProcessor) = idAndProcessor
      val newProcessor = if (oldProcessor.jar == null) {
        oldProcessor.copy(jar = appContext.appJar.orNull)
      } else {
        oldProcessor
      }
      (id, newProcessor)
    }
    DAG(dag.version, updated, dag.graph)
  }
}

object AppMaster {

  /** Master node doesn't return resource in time */
  case object AllocateResourceTimeOut

  /** Query task ActorRef by providing the taskId */
  case class LookupTaskActorRef(taskId: TaskId)

  case class TaskActorRef(task: ActorRef)

  class ServiceNotAvailableException(reason: String) extends Exception(reason)

  case class ExecutorBrief(
      executorId: ExecutorId, executor: String, workerId: WorkerId, status: String)

}