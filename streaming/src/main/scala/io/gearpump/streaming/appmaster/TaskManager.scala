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

package io.gearpump.streaming.appmaster

import akka.actor._
import akka.pattern.ask
import io.gearpump.TimeStamp
import io.gearpump.cluster.MasterToAppMaster.ReplayFromTimestampWindowTrailingEdge
import io.gearpump.streaming.AppMasterToExecutor._
import io.gearpump.streaming.ExecutorToAppMaster.{MessageLoss, RegisterTask}
import io.gearpump.streaming._
import io.gearpump.streaming.appmaster.AppMaster.{AllocateResourceTimeOut, LookupTaskActorRef, TaskActorRef}
import io.gearpump.streaming.appmaster.ClockService.{ChangeToNewDAGSuccess, ChangeToNewDAG}
import io.gearpump.streaming.appmaster.DagManager.{GetLatestDAG, GetTaskLaunchData, LatestDAG, NewDAGDeployed, TaskLaunchData, WatchChange}
import io.gearpump.streaming.appmaster.ExecutorManager.{ExecutorStarted, StartExecutorsTimeOut, _}
import io.gearpump.streaming.appmaster.TaskManager._
import io.gearpump.streaming.appmaster.TaskRegistry.{Accept, TaskLocation}
import io.gearpump.streaming.executor.Executor.RestartTasks
import io.gearpump.streaming.executor.ExecutorRestartPolicy
import io.gearpump.streaming.task._
import io.gearpump.streaming.util.ActorPathUtil
import io.gearpump.util.{Constants, LogUtil}
import org.slf4j.Logger

import scala.concurrent.Future
import scala.concurrent.duration._

private[appmaster] class TaskManager(
    appId: Int,
    dagManager: ActorRef,
    jarScheduler: JarScheduler,
    executorManager: ActorRef,
    clockService: ActorRef,
    appMaster: ActorRef,
    appName: String)
  extends Actor {

  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)
  val systemConfig = context.system.settings.config

  private val ids = new SessionIdFactory()

  private val executorRestartPolicy = new ExecutorRestartPolicy(maxNrOfRetries = 5, withinTimeRange = 20 seconds)
  implicit val timeout = Constants.FUTURE_TIMEOUT
  implicit val actorSystem = context.system
  import context.dispatcher

  dagManager ! WatchChange(watcher = self)
  executorManager ! SetTaskManager(self)

  private def getStartClock: Future[TimeStamp] = {
    (clockService ? GetStartClock).asInstanceOf[Future[StartClock]].map(_.clock)
  }

  private var startClock: Future[TimeStamp] = getStartClock

  def receive: Receive = (applicationReady(DagReadyState.empty))

  private def onClientQuery(taskRegistry: TaskRegistry): Receive = {
    case clock: ClockEvent =>
      clockService forward clock
    case GetTaskList =>
      sender ! TaskList(taskRegistry.getTaskExecutorMap)
    case LookupTaskActorRef(taskId) =>
      val executorId = taskRegistry.getExecutorId(taskId)
      val requestor = sender
      executorId.map { executorId =>
        val taskPath = ActorPathUtil.taskActorPath(appMaster, executorId, taskId)
        context.actorSelection(taskPath).resolveOne(3 seconds).map { taskActorRef =>
          requestor ! TaskActorRef(taskActorRef)
        }
      }
  }

  def applicationReady(state: DagReadyState): Receive = {
    executorManager ! state.taskRegistry.usedResource
    dagManager ! NewDAGDeployed(state.dag.version)
    dagManager ! GetLatestDAG
    LOG.info(s"goto state ApplicationReady(dag = ${state.dag.version})...")

    val recoverState = new StartDagState(state.dag, new TaskRegistry(state.dag.tasks))

    val onError: Receive = {
      case executorStopped@ExecutorStopped(executorId) =>
        if (state.taskRegistry.isTaskRegisteredForExecutor(executorId)) {
          self ! executorStopped
          context.become(recovery(recoverState))
        }
      case MessageLoss(executorId, taskId, cause) =>
        if (state.taskRegistry.isTaskRegisteredForExecutor(executorId) &&
          executorRestartPolicy.allowRestartExecutor(executorId)) {
          context.become(recovery(recoverState))
        } else {
          val errorMsg = s"Task $taskId fails too many times to recover"
          appMaster ! FailedToRecover(errorMsg)
        }
      case replay: ReplayFromTimestampWindowTrailingEdge =>
        LOG.error(s"Received $replay")
        context.become(recovery(recoverState))
    }

    val onNewDag: Receive = {
      case LatestDAG(newDag) =>
        import state.taskRegistry
        if (newDag.version > state.dag.version) {

          executorManager ! BroadCast(StartDynamicDag(newDag.version))
          LOG.info("Broadcasting StartDynamicDag")

          val dagDiff = migrate(state.dag, newDag)
          jarScheduler.setDag(newDag)
          val resourceRequestsDetails = jarScheduler.getRequestDetails()
          resourceRequestsDetails.foreach{ detail =>
            if(detail.requests.length > 0 && detail.requests.exists(!_.resource.isEmpty)){
              executorManager ! StartExecutors(detail.requests, detail.jar)
            }
          }

          var modifiedTasks = List.empty[TaskId]
          for (processorId <- (dagDiff.modifiedProcessors ++ dagDiff.impactedUpstream)) {
            val executors = taskRegistry.processorExecutors(processorId)
            executors.foreach { pair =>
              val (executorId, tasks) = pair
              modifiedTasks ++= tasks
              dagManager ! GetTaskLaunchData(newDag.version, processorId, ChangeTasksOnExecutor(executorId, tasks))
            }
          }

          val taskChangeRegistry = new TaskChangeRegistry(modifiedTasks)

          val startedTasks = state.taskRegistry.registeredTasks
          val dynamicTaskRegistration = new TaskRegistry(newDag.tasks, startedTasks)

          val nextState = new StartDagState(newDag, dynamicTaskRegistration, taskChangeRegistry)
          context.become(dynamicDag(nextState, recoverState))
        }
    }

    // recover to same version
    onClientQuery(state.taskRegistry) orElse onError orElse onNewDag orElse unHandled("applicationReady")
  }

  def dynamicDag(state: StartDagState, recoverState: StartDagState): Receive = {
    LOG.info(s"DynamicDag transit to dag version: ${state.dag.version}...")

    val onMessageLoss: Receive = {
      case MessageLoss(executorId, taskId, cause) =>
        if (state.taskRegistry.isTaskRegisteredForExecutor(executorId) &&
          executorRestartPolicy.allowRestartExecutor(executorId)) {
          context.become(recovery(recoverState))
        } else {
          val errorMsg = s"Task $taskId fails too many times to recover"
          appMaster ! FailedToRecover(errorMsg)
        }
    }

    onMessageLoss orElse onClientQuery(state.taskRegistry) orElse
      startDag(state, recoverState) orElse unHandled("dynamicDag")
  }

  private def startDag(state: StartDagState, recoverState: StartDagState): Receive = {
    case executor: ExecutorStarted =>
      import executor.{boundedJar, executorId, resource, workerId}
      val taskIds = jarScheduler.scheduleTask(boundedJar.get, workerId, executorId, resource)
      LOG.info(s"Executor $executor has been started, start to schedule tasks: ${taskIds.mkString(",")}")

      taskIds.groupBy(_.processorId).foreach { pair =>
        val (processorId, tasks) = pair
        dagManager ! GetTaskLaunchData(state.dag.version, processorId, StartTasksOnExecutor(executor.executorId, tasks))
      }
    case StartExecutorsTimeOut =>
      appMaster ! AllocateResourceTimeOut
    case ExecutorStopped(executorId) =>
      if(executorRestartPolicy.allowRestartExecutor(executorId)) {
        val resourceRequestDetail = jarScheduler.executorFailed(executorId)
        executorManager ! StartExecutors(resourceRequestDetail.requests, resourceRequestDetail.jar)
      } else {
        val errorMsg = s"Executor restarted too many times to recover"
        appMaster ! FailedToRecover(errorMsg)
      }

    case TaskLaunchData(processorDescription, subscribers, command) =>
      command match {
        case StartTasksOnExecutor(executorId, tasks) =>
          LOG.info(s"Start tasks on Executor($executorId), tasks: " + tasks)
          val launchTasks = LaunchTasks(tasks, state.dag.version, processorDescription, subscribers)
          executorManager ! UniCast(executorId, launchTasks)
          tasks.foreach(executorRestartPolicy.addTaskToExecutor(executorId, _))
        case ChangeTasksOnExecutor(executorId, tasks) =>
          LOG.info("change Task on executor: " + executorId + ", tasks: " + tasks)
          val changeTasks = ChangeTasks(tasks, state.dag.version, processorDescription.life, subscribers)
          executorManager ! UniCast(executorId, changeTasks)
        case other =>
          LOG.error(s"severe error! we expect ExecutorStarted but get ${other.getClass.toString}")
      }
    case TasksLaunched =>
    // We will track all launched task by message RegisterTask
    case TasksChanged(tasks) =>
      tasks.foreach(task =>state.taskChangeRegistry.taskChanged(task))

      if (allTasksReady(state)) {
        broadcastLocations(state)
      }

    case RegisterTask(taskId, executorId, host) =>
      val client = sender
      val register = state.taskRegistry
      val status = register.registerTask(taskId, TaskLocation(executorId, host))
      if (status == Accept) {
        LOG.info(s"RegisterTask($taskId) TaskLocation: $host, Executor: $executorId")
        val sessionId = ids.newSessionId

        startClock.foreach(clock => client ! TaskRegistered(taskId, sessionId, clock))
        if (allTasksReady(state)) {
          broadcastLocations(state)
        }
      } else {
        sender ! TaskRejected(taskId)
      }

    case TaskChanged(taskId, dagVersion) =>
      state.taskChangeRegistry.taskChanged(taskId)
      if (allTasksReady(state)) {
        broadcastLocations(state)
      }
    case locationReceived: TaskLocationsReceived =>
      state.executorReadyRegistry.registerExecutor(locationReceived.executorId)
      if (allTasksReady(state) &&
        state.executorReadyRegistry.allRegistered(state.taskRegistry.executors)) {
        LOG.info("All executors are ready to start...")
        clockService ! ChangeToNewDAG(state.dag)
      }
    case locationRejected: TaskLocationsRejected =>
      LOG.error(s"received $locationRejected, start to recover")
      context.become(recovery(recoverState))

    case ChangeToNewDAGSuccess(_) =>
      if (allTasksReady(state) &&
        state.executorReadyRegistry.allRegistered(state.taskRegistry.executors)) {
        executorManager ! BroadCast(StartAllTasks(state.dag.version))
        context.become(applicationReady(new DagReadyState(state.dag, state.taskRegistry)))
      }
  }

  private def allTasksReady(state: StartDagState): Boolean = {
    import state.{taskChangeRegistry, taskRegistry}
    taskRegistry.isAllTasksRegistered && taskChangeRegistry.allTaskChanged
  }

  private def broadcastLocations(state: StartDagState): Unit = {
    import state.{taskChangeRegistry, taskRegistry}
    LOG.info(s"All tasks have been launched; send Task locations to all executors")
    val taskLocations = taskRegistry.getTaskLocations
    executorManager ! BroadCast(TaskLocationsReady(taskLocations, state.dag.version))
  }

  def recovery(state: StartDagState): Receive = {
    val recoverDagVersion = state.dag.version
    executorManager ! BroadCast(RestartTasks(recoverDagVersion))
    jarScheduler.setDag(state.dag)

    // Use new Start Clock so that we recover at timepoint we fails.
    startClock = getStartClock
    LOG.info(s"goto state Recovery(recoverDag = $recoverDagVersion)...")
    val ignoreClock: Receive = {
      case clock: ClockEvent =>
      //ignore clock events.
    }

    if (state.dag.isEmpty) {
      applicationReady(new DagReadyState(state.dag, state.taskRegistry))
    } else {
      val recoverState = new StartDagState(state.dag, new TaskRegistry(state.dag.tasks))
      ignoreClock orElse startDag(state, recoverState) orElse unHandled("recovery")
    }
  }

  private def unHandled(state: String): Receive = {
    case other =>
      LOG.info(s"Received unknown message $other in state $state")
  }
}

private [appmaster] object TaskManager {

  class DagReadyState(
    val dag: DAG,
    val taskRegistry: TaskRegistry)

  object DagReadyState {
    def empty: DagReadyState = {
      new DagReadyState(
        DAG.empty().copy(version = -1),
        new TaskRegistry(List.empty[TaskId]))
    }
  }

  class StartDagState(
    val dag: DAG,
    val taskRegistry: TaskRegistry,
    val taskChangeRegistry: TaskChangeRegistry = new TaskChangeRegistry(List.empty[TaskId]),
    val executorReadyRegistry: ExecutorRegistry = new ExecutorRegistry)

  case object GetTaskList

  case class TaskList(tasks: Map[TaskId, ExecutorId])

  case class FailedToRecover(errorMsg: String)

  case class StartTasksOnExecutor(executorId: Int, tasks: List[TaskId])
  case class ChangeTasksOnExecutor(executorId: Int, tasks: List[TaskId])

  class ExecutorRegistry {
    private var registeredExecutors = Set.empty[ExecutorId]

    def registerExecutor(executorId: ExecutorId): Unit = {
      registeredExecutors += executorId
    }

    def allRegistered(all: List[ExecutorId]): Boolean = {
      all.forall(executor => registeredExecutors.contains(executor))
    }
  }

  class TaskChangeRegistry(targetTasks: List[TaskId]) {
    private var registeredTasks = Set.empty[TaskId]
    def taskChanged(taskId: TaskId): Unit = {
      registeredTasks += taskId
    }
    def allTaskChanged: Boolean = {
      targetTasks.forall(taskId => registeredTasks.contains(taskId))
    }
  }

  object TaskChangeRegistry {
    def empty: TaskChangeRegistry = new TaskChangeRegistry(List.empty[TaskId])
  }

  case object CheckApplicationReady

  case class DAGDiff(
    addedProcessors: List[ProcessorId],
    modifiedProcessors: List[ProcessorId],
    impactedUpstream: List[ProcessorId])

  def migrate(leftDAG: DAG, rightDAG: DAG): DAGDiff = {
    val left = leftDAG.processors.keySet
    val right = rightDAG.processors.keySet

    val added = right -- left
    val join = right -- added

    val modified = join.filter {processorId =>
      leftDAG.processors(processorId) != rightDAG.processors(processorId)
    }

    val upstream = (list: Set[ProcessorId]) => {
      (list).flatMap {processorId =>
        rightDAG.graph.incomingEdgesOf(processorId).map(_._1).toSet
      } -- list
    }

    val impactedUpstream = upstream(added ++ modified)

    // all upstream will be affected.
    DAGDiff(added.toList, modified.toList, impactedUpstream.toList)
  }

  class SessionIdFactory {
    private var nextSessionId = 1

    final def newSessionId: Int = {
      val sessionId = nextSessionId
      nextSessionId += 1
      sessionId
    }
  }
}