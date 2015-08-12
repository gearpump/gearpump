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

package org.apache.gearpump.streaming.executor

import akka.actor.SupervisorStrategy.{Resume, Stop}
import akka.actor._
import com.typesafe.config.Config
import org.apache.gearpump.cluster.MasterToAppMaster.MessageLoss
import org.apache.gearpump.cluster.{ExecutorContext, UserConfig}
import org.apache.gearpump.metrics.Metrics.{ReportMetrics, DemandMoreMetrics}
import org.apache.gearpump.metrics.{Metrics, MetricsReporterService}
import org.apache.gearpump.serializer.KryoPool
import org.apache.gearpump.streaming.AppMasterToExecutor._
import org.apache.gearpump.streaming.ExecutorToAppMaster.RegisterExecutor
import org.apache.gearpump.streaming._
import org.apache.gearpump.streaming.appmaster.TaskRegistry.TaskLocations
import org.apache.gearpump.streaming.executor.Executor._
import org.apache.gearpump.streaming.executor.TaskLauncher.TaskArgument
import org.apache.gearpump.streaming.task.{Subscriber, TaskId}
import org.apache.gearpump.transport.{Express, HostPort}
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.{ActorUtil, LogUtil}
import org.slf4j.Logger

import scala.concurrent.duration._
import scala.language.postfixOps
import org.apache.commons.lang.exception.ExceptionUtils

class Executor(executorContext: ExecutorContext, userConf : UserConfig, launcher: ITaskLauncher)  extends Actor {

  def this(executorContext: ExecutorContext, userConf: UserConfig) = {
    this(executorContext, userConf, TaskLauncher(executorContext, userConf))
  }

  import context.dispatcher
  import executorContext.{appId, appMaster, executorId, resource, worker}

  private val LOG: Logger = LogUtil.getLogger(getClass, executor = executorId, app = appId)

  private val address = ActorUtil.getFullPath(context.system, self.path)

  private val kryoPool =  new KryoPool(context.system.asInstanceOf[ExtendedActorSystem])

  LOG.info(s"Executor ${executorId} has been started, start to register itself...")
  LOG.info(s"Executor actor path: ${ActorUtil.getFullPath(context.system, self.path)}")

  appMaster ! RegisterExecutor(self, executorId, resource, worker)
  context.watch(appMaster)

  private var tasks = Map.empty[TaskId, ActorRef]
  val systemConfig = context.system.settings.config

  val express = Express(context.system)

  val metricsEnabled = systemConfig.getBoolean(GEARPUMP_METRIC_ENABLED)

  if (metricsEnabled) {
    val metricsReportService = context.actorOf(Props(new MetricsReporterService(Metrics(context.system))))
    appMaster.tell(ReportMetrics, metricsReportService)
  }

  def receive : Receive = appMasterMsgHandler

  private def getTaskId(actorRef: ActorRef): Option[TaskId] = {
    tasks.find(_._2 == sender()).map(_._1)
  }

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: MsgLostException =>
        val cause =  s"We got MessageLossException from task ${getTaskId(sender)}, replaying application..."
        LOG.error(cause)
        LOG.info(s"sending to appMaster ${appMaster.path.toString}")
        appMaster ! MessageLoss(executorId, cause)
        Resume
      case _: RegisterTaskFailedException =>
        val taskId = getTaskId(sender)
        LOG.error(s"We got RegisterTaskFailedException from ${taskId}, stopping this task...")
        Stop
      case ex: Throwable =>
        val taskId = getTaskId(sender)
        val errorMsg = s"We got ${ex.getClass.getName} from ${taskId}, we will treat it as MessageLoss, so that the system will replay all lost message"
        LOG.error(errorMsg, ex)
        val detailErrorMsg = errorMsg + "\n" + ExceptionUtils.getStackTrace(ex)
        appMaster ! MessageLoss(executorId, detailErrorMsg)
        Resume
    }

  private def launchTask(taskId: TaskId, argument: TaskArgument): ActorRef = {
    launcher.launch(List(taskId), argument, context, kryoPool).values.head
  }

  private val taskArgumentStore = new TaskArgumentStore()

  def appMasterMsgHandler : Receive = terminationWatch orElse {
    case LaunchTasks(taskIds, dagVersion, processorDescription, subscribers: List[Subscriber]) => {
      LOG.info(s"Launching Task $taskIds for app: ${appId}")
      val taskArgument = TaskArgument(dagVersion, processorDescription, subscribers)
      taskIds.foreach(taskArgumentStore.add(_, taskArgument))
      val newAdded = launcher.launch(taskIds, taskArgument, context, kryoPool)
      newAdded.foreach { newAddedTask =>
        context.watch(newAddedTask._2)
      }
      tasks ++= newAdded
      sender ! TasksLaunched
    }

    case ChangeTasks(taskIds, dagVersion, life, subscribers) =>
      LOG.info(s"Change Tasks $taskIds for app: ${appId}, verion: $life, $dagVersion, $subscribers")
      taskIds.foreach { taskId =>
        for (taskArgument <- taskArgumentStore.get(dagVersion, taskId)) {
          val processorDescription = taskArgument.processorDescription.copy(life = life)
          taskArgumentStore.add(taskId, TaskArgument(dagVersion, processorDescription, subscribers))
        }

        val taskActor = tasks.get(taskId)
        taskActor.foreach(_ forward ChangeTask(taskId, dagVersion, life, subscribers))
      }
      sender ! TasksChanged

    case TaskLocations(locations) =>
      LOG.info(s"TaskLocations Ready...")
      val result = locations.filter(location => !location._1.equals(express.localHost)).flatMap { kv =>
        val (host, taskIdList) = kv
        taskIdList.map(taskId => (TaskId.toLong(taskId), host))
      }
      express.startClients(locations.keySet).map { _ =>
        express.remoteAddressMap.send(result)
        express.remoteAddressMap.future().map{_ =>
          tasks.foreach { task =>
            task._2 ! TaskLocationReady
          }
        }
      }
    case RestartTasks(dagVersion) =>
      LOG.info(s"Executor received restart tasks")
      express.remoteAddressMap.send(Map.empty[Long, HostPort])

      tasks.foreach { task =>
        task._2 ! PoisonPill
      }
      context.become(restartingTask(dagVersion, remain = tasks.keys.size, restarted = Map.empty[TaskId, ActorRef]))

    case get: GetExecutorSummary =>
      val logFile = LogUtil.applicationLogDir(systemConfig)
      val processorTasks = tasks.keySet.groupBy(_.processorId).mapValues(_.toList).view.force
      sender ! ExecutorSummary(executorId, worker.workerId, address, logFile.getAbsolutePath, "active", tasks.size, processorTasks)

    case query: QueryExecutorConfig =>
      sender ! ExecutorConfig(systemConfig)
  }

  def restartingTask(dagVersion: Int, remain: Int, restarted: Map[TaskId, ActorRef]): Receive = terminationWatch orElse {
    case TaskStopped(actor) =>
      for ((taskId, _) <- (tasks.find(_._2 == actor))) {
        for (taskArgument <- taskArgumentStore.get(dagVersion, taskId)) {
          val task = launchTask(taskId, taskArgument)
          context.watch(task)
          val newRestarted = restarted + (taskId -> task)
          val newRemain = remain - 1
          if (newRemain == 0) {
            tasks ++= newRestarted
            context.become(appMasterMsgHandler)
          } else {
            context.become(restartingTask(dagVersion, newRemain, newRestarted))
          }
        }
      }
  }

  val terminationWatch: Receive = {
    case Terminated(actor) => {
      if (actor.compareTo(appMaster) == 0) {

        LOG.info(s"AppMaster ${appMaster.path.toString} is terminated, shutting down current executor $appId, $executorId")
        context.stop(self)
      } else {
        self ! TaskStopped(actor)
      }
    }
  }
}

object Executor {
  case class RestartTasks(dagVersion: Int)

  case object TaskLocationReady

  class TaskArgumentStore {

    private var store = Map.empty[TaskId, List[TaskArgument]]

    def add(taskId: TaskId, task: TaskArgument): Unit = {
      val list = store.getOrElse(taskId, List.empty[TaskArgument])
      store += taskId -> (task :: list)
    }

    def get(dagVersion: Int, taskId: TaskId): Option[TaskArgument] = {
      store.get(taskId).flatMap { list =>
        list.find { arg =>
          arg.dagVersion <= dagVersion
        }
      }
    }

    /**
     * when the new DAG is successfully deployed, then we should remove obsolete TaskArgument of old DAG.
     */
    def removeObsoleteVersion: Unit = {
      store = store.map{ kv =>
        val (k, list) = kv
        (k, list.take(1))
      }
    }
  }

  case class TaskStopped(task: ActorRef)

  case class ExecutorSummary(
    id: Int,
    workerId: Int,
    actorPath: String,
    logFile: String,
    status: String,
    taskCount: Int,
    tasks: Map[ProcessorId, List[TaskId]]
  )

  object ExecutorSummary {
    def empty: ExecutorSummary = ExecutorSummary(0, 0, "", "", "", 1, null)
  }

  case class GetExecutorSummary(executorId: Int)

  case class QueryExecutorConfig(executorId: Int)

  case class ExecutorConfig(config: Config)

}