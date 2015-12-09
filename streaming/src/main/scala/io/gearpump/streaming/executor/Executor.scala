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

  private var tasks = Map.empty[TaskId, ActorRef]
  private val taskArgumentStore = new TaskArgumentStore()

  val express = Express(context.system)

  val metricsEnabled = systemConfig.getBoolean(GEARPUMP_METRIC_ENABLED)

  if (metricsEnabled) {
    // register jvm metrics
    Metrics(context.system).register(new JvmMetricsSet(s"app${appId}.executor${executorId}"))

    val metricsReportService = context.actorOf(Props(new MetricsReporterService(Metrics(context.system))))
    appMaster.tell(ReportMetrics, metricsReportService)
  }

  private val NOT_INITIALIZED = -1
  def receive : Receive = applicationReady(dagVersion = NOT_INITIALIZED)

  private def getTaskId(actorRef: ActorRef): Option[TaskId] = {
    tasks.find{ kv =>
      kv._2 == actorRef
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

  private def assertVersion(expectVersion: Int, version: Int, clue: Any): Unit = {
    if (expectVersion != version) {
      val errorMessage = s"Version mismatch: we expect dag version $expectVersion, but get $version; clue: $clue"
      LOG.error(errorMessage)
      throw new DagVersionMismatchException(errorMessage)
    }
  }

  def dynamicDag(dagVersion: Int, launched: List[TaskId], changed: List[ChangeTask]): Receive = executorService orElse {
    case launch@ LaunchTasks(taskIds, version, processorDescription, subscribers: List[Subscriber]) => {
      assertVersion(dagVersion, version, clue = launch)

      LOG.info(s"Launching Task $taskIds for app: ${appId}")
      val taskArgument = TaskArgument(version, processorDescription, subscribers)
      taskIds.foreach(taskArgumentStore.add(_, taskArgument))
      val newAdded = launcher.launch(taskIds, taskArgument, context, serializerPool)
      newAdded.foreach { newAddedTask =>
        context.watch(newAddedTask._2)
      }
      tasks ++= newAdded
      sender ! TasksLaunched
      context.become(dynamicDag(version, launched ++ taskIds, changed))
    }
    case change@ChangeTasks(taskIds, version, life, subscribers) =>
      assertVersion(dagVersion, version, clue = change)

      LOG.info(s"Change Tasks $taskIds for app: ${appId}, verion: $life, $dagVersion, $subscribers")

      val newChangedTasks = taskIds.map { taskId =>
        for (taskArgument <- taskArgumentStore.get(dagVersion, taskId)) {
          val processorDescription = taskArgument.processorDescription.copy(life = life)
          taskArgumentStore.add(taskId, TaskArgument(dagVersion, processorDescription, subscribers))
        }
        ChangeTask(taskId, dagVersion, life, subscribers)
      }
      sender ! TasksChanged(taskIds)
      context.become(dynamicDag(dagVersion, launched, changed ++ newChangedTasks))

    case startAll@StartAllTasks(taskLocations, startClock, version) =>
      LOG.info(s"TaskLocations Ready...")
      assertVersion(dagVersion, version, clue = startAll)

      val result = taskLocations.locations.filter(location => !location._1.equals(express.localHost)).flatMap { kv =>
        val (host, taskIdList) = kv
        taskIdList.map(taskId => (TaskId.toLong(taskId), host))
      }
      express.startClients(taskLocations.locations.keySet).map { _ =>
        express.remoteAddressMap.send(result)
        express.remoteAddressMap.future().map { _ =>
          launched.foreach(taskId => tasks.get(taskId).foreach(_ ! StartTask(taskId)))
          changed.foreach(changeTask => tasks.get(changeTask.taskId).foreach(_ ! changeTask))
        }
      }
      taskArgumentStore.removeNewerVersion(dagVersion)
      taskArgumentStore.removeObsoleteVersion
      context.become(applicationReady(dagVersion))

    case registered: TaskRegistered =>
      tasks.get(registered.taskId).foreach {
        case actorRef: ActorRef =>
          tasks += registered.taskId -> actorRef
          actorRef forward registered
      }
    case rejected: TaskRejected =>
      tasks.get(rejected.taskId).foreach {
        case task: ActorRef => task ! PoisonPill
      }
      tasks -= rejected.taskId
      LOG.error(s"Task ${rejected.taskId} is rejected by AppMaster, shutting down it...")

    case register: RegisterTask =>
      sendMsgWithTimeOutCallBack(appMaster, register, registerTaskTimeout, timeOutHandler(register.taskId))
  }

  def applicationReady(dagVersion: Int): Receive = executorService orElse {
    case launch: LaunchTasks =>
      LOG.info(s"received $launch, Executor transit to dag version: ${launch.dagVersion} from current version $dagVersion")
      context.become(dynamicDag(launch.dagVersion, List.empty[TaskId], List.empty[ChangeTask]))
      self forward launch

    case change: ChangeTasks =>
      LOG.info(s"received $change, Executor transit to dag version: ${change.dagVersion} from current version $dagVersion")
      context.become(dynamicDag(change.dagVersion, List.empty[TaskId], List.empty[ChangeTask]))
      self forward change
  }

  private val timeOutHandler = (taskId: TaskId) => {
    val cause = s"Failed to register task $taskId to AppMaster of application $appId, " +
      s"executor id is $executorId, treat it as MessageLoss"
    LOG.error(cause)
    appMaster ! MessageLoss(executorId, taskId, cause)
  }

  def restartingTasks(dagVersion: Int, remain: Int, needRestart: List[TaskId]): Receive = executorService orElse {
    case TaskStopped(actor) =>
      for (taskId <- getTaskId(actor)) {
        if (taskArgumentStore.get(dagVersion, taskId).nonEmpty) {
          val newNeedRestart = needRestart :+ taskId
          val newRemain = remain - 1
          if (newRemain == 0) {
            val newRestarted = newNeedRestart.map{ taskId_ =>
              taskId_ -> launchTask(taskId_, taskArgumentStore.get(dagVersion, taskId_).get)
            }.toMap

            tasks = newRestarted
            context.become(dynamicDag(dagVersion, newNeedRestart, List.empty[ChangeTask]))
          } else {
            context.become(restartingTasks(dagVersion, newRemain, newNeedRestart))
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

  def onRestartTasks: Receive = {
    case RestartTasks(dagVersion) =>
      LOG.info(s"Executor received restart tasks")
      val tasksToRestart = tasks.keys.count(taskArgumentStore.get(dagVersion, _).nonEmpty)
      express.remoteAddressMap.send(Map.empty[Long, HostPort])
      context.become(restartingTasks(dagVersion, remain = tasksToRestart, needRestart = List.empty[TaskId]))

      tasks.values.foreach {
        case task: ActorRef => task ! PoisonPill
      }
  }

  def executorService: Receive = terminationWatch orElse onRestartTasks orElse {
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

  class DagVersionMismatchException(msg: String) extends Exception(msg)
}