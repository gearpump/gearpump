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

package io.gearpump.streaming.executor

import java.lang.management.ManagementFactory

import akka.actor.SupervisorStrategy.Resume
import akka.actor._
import com.typesafe.config.Config
import io.gearpump.cluster.{ExecutorContext, UserConfig}
import io.gearpump.metrics.Metrics.ReportMetrics
import io.gearpump.metrics.{JvmMetricsSet, Metrics, MetricsReporterService}
import io.gearpump.serializer.SerializationFramework
import io.gearpump.streaming.AppMasterToExecutor.{MsgLostException, TasksChanged, TasksLaunched, _}
import io.gearpump.streaming.Constants._
import io.gearpump.streaming.ExecutorToAppMaster.{MessageLoss, RegisterExecutor, RegisterTask}
import io.gearpump.streaming.ProcessorId
import io.gearpump.streaming.executor.Executor._
import io.gearpump.streaming.executor.TaskLauncher.TaskArgument
import io.gearpump.streaming.task.{Subscriber, TaskId}
import io.gearpump.streaming.task.TaskActor._
import io.gearpump.transport.{Express, HostPort}
import io.gearpump.util.Constants._
import io.gearpump.util.{ActorUtil, Constants, LogUtil, TimeOutScheduler}
import org.apache.commons.lang.exception.ExceptionUtils
import org.slf4j.Logger

import scala.concurrent.duration._
import scala.language.postfixOps

class Executor(executorContext: ExecutorContext, userConf : UserConfig, launcher: ITaskLauncher)
  extends Actor with TimeOutScheduler{

  def this(executorContext: ExecutorContext, userConf: UserConfig) = {
    this(executorContext, userConf, TaskLauncher(executorContext, userConf))
  }

  import context.dispatcher
  import executorContext.{appId, appMaster, executorId, resource, worker}

  private val LOG: Logger = LogUtil.getLogger(getClass, executor = executorId, app = appId)

  implicit val timeOut = FUTURE_TIMEOUT
  private val address = ActorUtil.getFullPath(context.system, self.path)
  private val systemConfig = context.system.settings.config
  private val serializerPool = getSerializerPool()
  private val registerTaskTimeout = systemConfig.getLong(GEARPUMP_STREAMING_REGISTER_TASK_TIMEOUT_MS)

  LOG.info(s"Executor ${executorId} has been started, start to register itself...")
  LOG.info(s"Executor actor path: ${ActorUtil.getFullPath(context.system, self.path)}")

  appMaster ! RegisterExecutor(self, executorId, resource, worker)
  context.watch(appMaster)

  private var tasks = Map.empty[TaskId, (ActorRef, Int)]
  private val taskArgumentStore = new TaskArgumentStore()

  val express = Express(context.system)

  val metricsEnabled = systemConfig.getBoolean(GEARPUMP_METRIC_ENABLED)

  if (metricsEnabled) {
    // register jvm metrics
    Metrics(context.system).register(new JvmMetricsSet(s"app${appId}.executor${executorId}"))

    val metricsReportService = context.actorOf(Props(new MetricsReporterService(Metrics(context.system))))
    appMaster.tell(ReportMetrics, metricsReportService)
  }

  def receive : Receive = launchTasksHandler orElse terminationWatch

  private def getTaskId(actorRef: ActorRef): Option[TaskId] = {
    tasks.find{ kv =>
      val (_, (_actorRef, _)) = kv
      _actorRef == actorRef
    }.map(_._1)
  }

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: MsgLostException =>
        val taskId = getTaskId(sender)
        val cause =  s"We got MessageLossException from task ${getTaskId(sender)}, replaying application..."
        LOG.error(cause)
        taskId.foreach(appMaster ! MessageLoss(executorId, _,  cause))
        Resume
      case ex: Throwable =>
        val taskId = getTaskId(sender)
        val errorMsg = s"We got ${ex.getClass.getName} from $taskId, we will treat it as MessageLoss, so that the system will replay all lost message"
        LOG.error(errorMsg, ex)
        val detailErrorMsg = errorMsg + "\n" + ExceptionUtils.getStackTrace(ex)
        taskId.foreach(appMaster ! MessageLoss(executorId, _, detailErrorMsg))
        Resume
    }

  private def launchTask(taskId: TaskId, argument: TaskArgument): ActorRef = {
    launcher.launch(List(taskId), argument, context, serializerPool).values.head
  }

  def launchTasksHandler: Receive = queryMsgHandler orElse {
    case LaunchTasks(taskIds, dagVersion, processorDescription, subscribers: List[Subscriber]) => {
      LOG.info(s"Launching Task $taskIds for app: ${appId}")
      val taskArgument = TaskArgument(dagVersion, processorDescription, subscribers)
      taskIds.foreach(taskArgumentStore.add(_, taskArgument))
      val newAdded = launcher.launch(taskIds, taskArgument, context, serializerPool)
      newAdded.foreach { newAddedTask =>
        context.watch(newAddedTask._2)
      }
      tasks ++= newAdded.mapValues((_, NONE_SESSION))
      sender ! TasksLaunched
    }

    case StartAllTasks(taskLocations, startClock, dagVersion) =>
      LOG.info(s"TaskLocations Ready...")
      val result = taskLocations.locations.filter(location => !location._1.equals(express.localHost)).flatMap { kv =>
        val (host, taskIdList) = kv
        taskIdList.map(taskId => (TaskId.toLong(taskId), host))
      }
      express.startClients(taskLocations.locations.keySet).map { _ =>
        express.remoteAddressMap.send(result)
        express.remoteAddressMap.future().map { _ =>
          tasks.values.foreach {
            case (actor, sessionId) => actor ! Start(startClock, sessionId)
          }
        }
      }
      taskArgumentStore.removeNewerVersion(dagVersion)
      taskArgumentStore.removeObsoleteVersion
      context.become(applicationReady orElse terminationWatch)

    case registered: TaskRegistered =>
      tasks.get(registered.taskId).foreach {
        case (actorRef, sessionId) =>
          tasks += registered.taskId -> (actorRef, registered.sessionId)
      }
    case rejected: TaskRejected =>
      tasks.get(rejected.taskId).foreach {
        case (task, sessionId) => task ! PoisonPill
      }
      tasks -= rejected.taskId
      LOG.error(s"Task ${rejected.taskId} is rejected by AppMaster, shutting down it...")

    case register: RegisterTask =>
      sendMsgWithTimeOutCallBack(appMaster, register, registerTaskTimeout, timeOutHandler(register.taskId))
  }

  def applicationReady: Receive = queryMsgHandler orElse {
    case ChangeTasks(taskIds, dagVersion, life, subscribers) =>
      LOG.info(s"Change Tasks $taskIds for app: ${appId}, verion: $life, $dagVersion, $subscribers")
      taskIds.foreach { taskId =>
        for (taskArgument <- taskArgumentStore.get(dagVersion, taskId)) {
          val processorDescription = taskArgument.processorDescription.copy(life = life)
          taskArgumentStore.add(taskId, TaskArgument(dagVersion, processorDescription, subscribers))
        }

        val taskActor = tasks.get(taskId)
        taskActor.foreach(_._1 forward ChangeTask(taskId, dagVersion, life, subscribers))
      }
      sender ! TasksChanged
      context.become(launchTasksHandler orElse terminationWatch)

    case RestartTasks(dagVersion) =>
      LOG.info(s"Executor received restart tasks")
      val tasksToRestart = tasks.keys.count(taskArgumentStore.get(dagVersion, _).nonEmpty)
      express.remoteAddressMap.send(Map.empty[Long, HostPort])
      context.become(restartingTask(dagVersion, remain = tasksToRestart, needRestart = List.empty[TaskId]))

      tasks.values.foreach {
        case (task, sessionId) => task ! PoisonPill
      }
  }

  def queryMsgHandler: Receive = {
    case get: GetExecutorSummary =>
      val logFile = LogUtil.applicationLogDir(systemConfig)
      val processorTasks = tasks.keySet.groupBy(_.processorId).mapValues(_.toList).view.force
      sender ! ExecutorSummary(
        executorId,
        worker.workerId,
        address,
        logFile.getAbsolutePath,
        "active",
        tasks.size,
        processorTasks,
        jvmName = ManagementFactory.getRuntimeMXBean().getName())

    case query: QueryExecutorConfig =>
      sender ! ExecutorConfig(systemConfig)
  }

  private val timeOutHandler = (taskId: TaskId) => {
    val cause = s"Failed to register task $taskId to AppMaster of application $appId, " +
      s"executor id is $executorId, treat it as MessageLoss"
    LOG.error(cause)
    appMaster ! MessageLoss(executorId, taskId, cause)
  }

  def restartingTask(dagVersion: Int, remain: Int, needRestart: List[TaskId]): Receive = terminationWatch orElse {
    case TaskStopped(actor) =>
      for (taskId <- getTaskId(actor)) {
        if (taskArgumentStore.get(dagVersion, taskId).nonEmpty) {
          val newNeedRestart = needRestart :+ taskId
          val newRemain = remain - 1
          if (newRemain == 0) {
            val newRestarted = newNeedRestart.map{ taskId_ =>
              taskId_ -> launchTask(taskId_, taskArgumentStore.get(dagVersion, taskId_).get)
            }.toMap

            tasks = newRestarted.mapValues((_, NONE_SESSION))
            context.become(launchTasksHandler orElse terminationWatch)
          } else {
            context.become(restartingTask(dagVersion, newRemain, newNeedRestart))
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

  private def getSerializerPool(): SerializationFramework = {
    val system = context.system.asInstanceOf[ExtendedActorSystem]
    val clazz = Class.forName(systemConfig.getString(Constants.GEARPUMP_SERIALIZER_POOL))
    val pool = clazz.newInstance().asInstanceOf[SerializationFramework]
    pool.init(system, userConf)
    pool.asInstanceOf[SerializationFramework]
  }
}

object Executor {
  case class RestartTasks(dagVersion: Int)

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

    def removeNewerVersion(currentVersion: Int): Unit = {
      store = store.map{ kv =>
        val (k, list) = kv
        (k, list.filter(_.dagVersion <= currentVersion))
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
    tasks: Map[ProcessorId, List[TaskId]],
    jvmName: String
  )

  object ExecutorSummary {
    def empty: ExecutorSummary = ExecutorSummary(0, 0, "", "", "", 1, null, jvmName = "")
  }

  case class GetExecutorSummary(executorId: Int)

  case class QueryExecutorConfig(executorId: Int)

  case class ExecutorConfig(config: Config)
}