/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.streaming.executor

import akka.actor._
import akka.actor.SupervisorStrategy.Resume
import com.typesafe.config.Config
import io.gearpump.cluster.{ClusterConfig, ExecutorContext, UserConfig}
import io.gearpump.cluster.worker.WorkerId
import io.gearpump.metrics.{JvmMetricsSet, Metrics, MetricsReporterService}
import io.gearpump.metrics.Metrics.ReportMetrics
import io.gearpump.serializer.SerializationFramework
import io.gearpump.streaming.AppMasterToExecutor.{MsgLostException, TasksChanged, TasksLaunched, _}
import io.gearpump.streaming.ExecutorToAppMaster.{MessageLoss, RegisterExecutor, RegisterTask, UnRegisterTask}
import io.gearpump.streaming.ProcessorId
import io.gearpump.streaming.executor.Executor._
import io.gearpump.streaming.executor.TaskLauncher.TaskArgument
import io.gearpump.streaming.task.{Subscriber, TaskId}
import io.gearpump.transport.{Express, HostPort}
import io.gearpump.util.{ActorUtil, Constants, LogUtil, TimeOutScheduler}
import io.gearpump.util.Constants._
import java.lang.management.ManagementFactory
import org.apache.commons.lang.exception.ExceptionUtils
import org.slf4j.Logger
import scala.concurrent.duration._

/**
 * Executor is child of AppMaster.
 * It usually represents a JVM process. It is a container for all tasks.
 */

// TODO: What if Executor stuck in state DynamicDag and cannot get out???
// For example, due to some message loss when there is network glitch.
// Executor will hang there for ever???
//
class Executor(executorContext: ExecutorContext, userConf : UserConfig, launcher: ITaskLauncher)
  extends Actor with TimeOutScheduler{

  def this(executorContext: ExecutorContext, userConf: UserConfig) = {
    this(executorContext, userConf, TaskLauncher(executorContext, userConf))
  }

  import context.dispatcher
  import executorContext.{appId, appMaster, executorId, resource, worker}

  private val LOG: Logger = LogUtil.getLogger(getClass, executor = executorId, app = appId)

  private val address = ActorUtil.getFullPath(context.system, self.path)
  private val systemConfig = context.system.settings.config
  private val serializerPool = getSerializerPool()
  private val taskDispatcher = systemConfig.getString(Constants.GEARPUMP_TASK_DISPATCHER)

  private var state = State.ACTIVE

  // States transition start, in unix time
  private var transitionStart = 0L
  // States transition end, in unix time
  private var transitionEnd = 0L
  private val transitWarningThreshold = 5000 // ms,

  // Starts health check Ticks
  self ! HealthCheck

  LOG.info(s"Executor $executorId has been started, start to register itself...")
  LOG.info(s"Executor actor path: ${ActorUtil.getFullPath(context.system, self.path)}")

  appMaster ! RegisterExecutor(self, executorId, resource, worker)
  context.watch(appMaster)

  private var tasks = Map.empty[TaskId, ActorRef]
  private val taskArgumentStore = new TaskArgumentStore()

  val express = Express(context.system)

  val metricsEnabled = systemConfig.getBoolean(GEARPUMP_METRIC_ENABLED)

  if (metricsEnabled) {
    // Registers jvm metrics
    Metrics(context.system).register(new JvmMetricsSet(s"app$appId.executor$executorId"))

    val metricsReportService = context.actorOf(Props(new MetricsReporterService(
      Metrics(context.system))))
    appMaster.tell(ReportMetrics, metricsReportService)
  }

  private val NOT_INITIALIZED = -1
  def receive: Receive = applicationReady(dagVersion = NOT_INITIALIZED)

  private def getTaskId(actorRef: ActorRef): Option[TaskId] = {
    tasks.find(_._2 == actorRef).map(_._1)
  }

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
      case _: MsgLostException =>
        val taskId = getTaskId(sender)
        val cause = s"We got MessageLossException from task ${getTaskId(sender)}, " +
          s"replaying application..."
        LOG.error(cause)
        taskId.foreach(appMaster ! MessageLoss(executorId, _, cause))
        Resume
      case ex: Throwable =>
        val taskId = getTaskId(sender)
        val errorMsg = s"We got ${ex.getClass.getName} from $taskId, we will treat it as" +
          s" MessageLoss, so that the system will replay all lost message"
        LOG.error(errorMsg, ex)
        val detailErrorMsg = errorMsg + "\n" + ExceptionUtils.getStackTrace(ex)
        taskId.foreach(appMaster ! MessageLoss(executorId, _, detailErrorMsg, Some(ex)))
        Resume
    }

  private def launchTask(taskId: TaskId, argument: TaskArgument): ActorRef = {
    launcher.launch(List(taskId), argument, context, serializerPool, taskDispatcher).values.head
  }

  private def assertVersion(expectVersion: Int, version: Int, clue: Any): Unit = {
    if (expectVersion != version) {
      val errorMessage = s"Version mismatch: we expect dag version $expectVersion, " +
        s"but get $version; clue: $clue"
      LOG.error(errorMessage)
      throw new DagVersionMismatchException(errorMessage)
    }
  }

  def dynamicDagPhase1(
      dagVersion: Int, launched: List[TaskId], changed: List[ChangeTask], registered: List[TaskId])
    : Receive = {
    state = State.DYNAMIC_DAG_PHASE1
    box({
      case launch@LaunchTasks(taskIds, version, processorDescription,
      subscribers: List[Subscriber]) => {
        assertVersion(dagVersion, version, clue = launch)

        LOG.info(s"Launching Task $taskIds for app: $appId")
        val taskArgument = TaskArgument(version, processorDescription, subscribers)
        taskIds.foreach(taskArgumentStore.add(_, taskArgument))
        val newAdded = launcher.launch(taskIds, taskArgument, context, serializerPool,
          taskDispatcher)
        newAdded.foreach { newAddedTask =>
          context.watch(newAddedTask._2)
        }
        tasks ++= newAdded
        sender ! TasksLaunched
        context.become(dynamicDagPhase1(version, launched ++ taskIds, changed, registered))
      }
      case change@ChangeTasks(taskIds, version, life, subscribers) =>
        assertVersion(dagVersion, version, clue = change)

        LOG.info(s"Change Tasks $taskIds for app: $appId, verion: $life, $dagVersion, $subscribers")

        val newChangedTasks = taskIds.map { taskId =>
          for (taskArgument <- taskArgumentStore.get(dagVersion, taskId)) {
            val processorDescription = taskArgument.processorDescription.copy(life = life)
            taskArgumentStore.add(taskId, TaskArgument(dagVersion, processorDescription,
              subscribers))
          }
          ChangeTask(taskId, dagVersion, life, subscribers)
        }
        sender ! TasksChanged(taskIds)
        context.become(dynamicDagPhase1(dagVersion, launched, changed ++ newChangedTasks,
          registered))

      case locations@TaskLocationsReady(taskLocations, version) =>
        LOG.info(s"TaskLocations Ready...")
        assertVersion(dagVersion, version, clue = locations)

        // Check whether all tasks has been registered.
        if ((launched.toSet -- registered.toSet).isEmpty) {
          // Confirm all tasks has been registered.
          val result = taskLocations.locations.filter {
            location => !location._1.equals(express.localHost)
          }.flatMap { kv =>
            val (host, taskIdList) = kv
            taskIdList.map(taskId => (TaskId.toLong(taskId), host))
          }

          val replyTo = sender
          express.startClients(taskLocations.locations.keySet).foreach { _ =>
            express.remoteAddressMap.send(result)
            express.remoteAddressMap.future().foreach { _ =>
              LOG.info(s"sending TaskLocationsReceived back to appmaster")
              replyTo ! TaskLocationsReceived(version, executorId)
            }
          }
          context.become(dynamicDagPhase2(dagVersion, launched, changed))
        } else {
          LOG.error("Inconsistency between AppMaser and Executor! AppMaster thinks DynamicDag " +
            "transition is ready, while Executor have not get all tasks registered, " +
            "that task will not be functional...")

          // Reject TaskLocations...
          val missedTasks = (launched.toSet -- registered.toSet).toList
          val errorMsg = "We have not received TaskRegistered for following tasks: " +
            missedTasks.mkString(", ")
          LOG.error(errorMsg)
          sender ! TaskLocationsRejected(dagVersion, executorId, errorMsg, null)
          // Stays with current status...
        }

      case confirm: TaskRegistered =>
        tasks.get(confirm.taskId).foreach {
          case actorRef: ActorRef =>
            tasks += confirm.taskId -> actorRef
            actorRef forward confirm
        }
        context.become(dynamicDagPhase1(dagVersion, launched, changed,
          registered :+ confirm.taskId))

      case rejected: TaskRejected =>
        // Means this task should not exist...
        tasks.get(rejected.taskId).foreach(_ ! PoisonPill)
        tasks -= rejected.taskId
        LOG.error(s"Task ${rejected.taskId} is rejected by AppMaster, shutting down it...")

      case register: RegisterTask =>
        appMaster ! register
    })
  }

  def dynamicDagPhase2(dagVersion: Int, launched: List[TaskId], changed: List[ChangeTask])
    : Receive = {
    LOG.info("Transit to dynamic Dag Phase2")
    state = State.DYNAMIC_DAG_PHASE2
    box {
      case startAll@StartAllTasks(version) =>
        LOG.info(s"Start All Tasks...")
        assertVersion(dagVersion, version, clue = startAll)

        launched.foreach(taskId => tasks.get(taskId).foreach(_ ! StartTask(taskId)))
        changed.foreach(changeTask => tasks.get(changeTask.taskId).foreach(_ ! changeTask))

        taskArgumentStore.removeNewerVersion(dagVersion)
        taskArgumentStore.removeObsoleteVersion
        context.become(applicationReady(dagVersion))
    }
  }

  def applicationReady(dagVersion: Int): Receive = {
    state = State.ACTIVE
    transitionEnd = System.currentTimeMillis()

    if (dagVersion != NOT_INITIALIZED) {
      LOG.info("Transit to state Application Ready. This transition takes " +
        (transitionEnd - transitionStart) + " milliseconds")
    }
    box {
      case start: StartDynamicDag =>
        LOG.info("received StartDynamicDag")
        if (start.dagVersion > dagVersion) {
          transitionStart = System.currentTimeMillis()
          LOG.info(s"received $start, Executor transit to dag version: ${start.dagVersion} from " +
            s"current version $dagVersion")
          context.become(dynamicDagPhase1(start.dagVersion, List.empty[TaskId],
            List.empty[ChangeTask], List.empty[TaskId]))
        }
      case launch: LaunchTasks =>
        if (launch.dagVersion > dagVersion) {
          transitionStart = System.currentTimeMillis()
          LOG.info(s"received $launch, Executor transit to dag " +
            s"version: ${launch.dagVersion} from current version $dagVersion")
          context.become(dynamicDagPhase1(launch.dagVersion, List.empty[TaskId],
            List.empty[ChangeTask], List.empty[TaskId]))
          self forward launch
        }

      case change: ChangeTasks =>
        if (change.dagVersion > dagVersion) {
          transitionStart = System.currentTimeMillis()
          LOG.info(s"received $change, Executor transit to dag version: ${change.dagVersion} from" +
            s" current version $dagVersion")
          context.become(dynamicDagPhase1(change.dagVersion, List.empty[TaskId],
            List.empty[ChangeTask], List.empty[TaskId]))
          self forward change
        }

      case StopTask(taskId) =>
        // Old soldiers never die, they just fade away ;)
        val fadeAwayTask = tasks.get(taskId)
        if (fadeAwayTask.isDefined) {
          context.stop(fadeAwayTask.get)
        }
        tasks -= taskId

      case unRegister: UnRegisterTask =>
        // Sends UnRegisterTask to AppMaster
        appMaster ! unRegister
    }
  }

  def restartingTasks(dagVersion: Int, remain: Int, needRestart: List[TaskId]): Receive = {
    state = State.RECOVERY
    box {
      case TaskStopped(actor) =>
        for (taskId <- getTaskId(actor)) {
          if (taskArgumentStore.get(dagVersion, taskId).nonEmpty) {
            val newNeedRestart = needRestart :+ taskId
            val newRemain = remain - 1
            if (newRemain == 0) {
              val newRestarted = newNeedRestart.map { taskId_ =>
                val taskActor = launchTask(taskId_, taskArgumentStore.get(dagVersion, taskId_).get)
                context.watch(taskActor)
                taskId_ -> taskActor
              }.toMap

              tasks = newRestarted
              context.become(dynamicDagPhase1(dagVersion, newNeedRestart, List.empty[ChangeTask],
                List.empty[TaskId]))
            } else {
              context.become(restartingTasks(dagVersion, newRemain, newNeedRestart))
            }
          }
        }
    }
  }

  val terminationWatch: Receive = {
    case Terminated(actor) =>
      if (actor.compareTo(appMaster) == 0) {
        LOG.info(s"AppMaster ${appMaster.path.toString} is terminated, shutting down current " +
          s"executor $appId, $executorId")
        context.stop(self)
      } else {
        self ! TaskStopped(actor)
      }
  }

  def onRestartTasks: Receive = {
    case RestartTasks(dagVersion) =>
      transitionStart = System.currentTimeMillis()
      LOG.info(s"Executor received restart tasks")
      val tasksToRestart = tasks.keys.count(taskArgumentStore.get(dagVersion, _).nonEmpty)
      express.remoteAddressMap.send(Map.empty[Long, HostPort])
      context.become(restartingTasks(dagVersion, remain = tasksToRestart,
        needRestart = List.empty[TaskId]))

      tasks.values.foreach {
        task: ActorRef => task ! PoisonPill
      }
  }

  def executorService: Receive = terminationWatch orElse onRestartTasks orElse {
    case _: TaskChanged => // Skip
    case _: GetExecutorSummary =>
      val logFile = LogUtil.applicationLogDir(systemConfig)
      val processorTasks = tasks.keySet.groupBy(_.processorId).mapValues(_.toList).view.force
      sender ! ExecutorSummary(
        executorId,
        worker.workerId,
        address,
        logFile.getAbsolutePath,
        state,
        tasks.size,
        processorTasks,
        jvmName = ManagementFactory.getRuntimeMXBean().getName())

    case _: QueryExecutorConfig =>
      sender ! ExecutorConfig(ClusterConfig.filterOutDefaultConfig(systemConfig))
    case HealthCheck =>
      context.system.scheduler.scheduleOnce(3.second)(HealthCheck)
      if (state != State.ACTIVE && (transitionEnd - transitionStart) > transitWarningThreshold) {
        LOG.error(s"Executor status: " + state +
          s", it takes too long(${transitionEnd - transitionStart}) to do transition")
      }
  }

  private def getSerializerPool(): SerializationFramework = {
    val system = context.system.asInstanceOf[ExtendedActorSystem]
    val clazz = Class.forName(systemConfig.getString(Constants.GEARPUMP_SERIALIZER_POOL))
    val pool = clazz.newInstance().asInstanceOf[SerializationFramework]
    pool.init(system, userConf)
    pool.asInstanceOf[SerializationFramework]
  }

  private def unHandled(state: String): Receive = {
    case other =>
      LOG.info(s"Received unknown message $other in state: $state")
  }

  private def box(receive: Receive): Receive = {
    executorService orElse receive orElse unHandled(state)
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
     * When the new DAG is successfully deployed, then we should remove obsolete
     * TaskArgument of old DAG.
     */
    def removeObsoleteVersion(): Unit = {
      store = store.map { kv =>
        val (k, list) = kv
        (k, list.take(1))
      }
    }

    def removeNewerVersion(currentVersion: Int): Unit = {
      store = store.map { kv =>
        val (k, list) = kv
        (k, list.filter(_.dagVersion <= currentVersion))
      }
    }
  }

  case class TaskStopped(task: ActorRef)

  case class ExecutorSummary(
      id: Int,
      workerId: WorkerId,
      actorPath: String,
      logFile: String,
      status: String,
      taskCount: Int,
      tasks: Map[ProcessorId, List[TaskId]],
      jvmName: String
  )

  object ExecutorSummary {
    def empty: ExecutorSummary = {
      ExecutorSummary(0, WorkerId.unspecified, "", "", "", 1, null, jvmName = "")
    }
  }

  case class GetExecutorSummary(executorId: Int)

  case class QueryExecutorConfig(executorId: Int)

  case class ExecutorConfig(config: Config)

  class DagVersionMismatchException(msg: String) extends Exception(msg)

  object State {
    val ACTIVE = "active"
    val DYNAMIC_DAG_PHASE1 = "dynamic_dag_phase1"
    val DYNAMIC_DAG_PHASE2 = "dynamic_dag_phase2"
    val RECOVERY = "dag_recovery"
  }

  object HealthCheck
}