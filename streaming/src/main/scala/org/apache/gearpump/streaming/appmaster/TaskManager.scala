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

import akka.actor._
import akka.pattern.ask
import org.apache.gearpump.cluster.MasterToAppMaster.{MessageLoss, ReplayFromTimestampWindowTrailingEdge}
import org.apache.gearpump.streaming.AppMasterToExecutor.{ChangeTasks, LaunchTasks, Start, TaskChanged, TaskRejected, TasksChanged, TasksLaunched}
import org.apache.gearpump.streaming.ExecutorToAppMaster.RegisterTask
import org.apache.gearpump.streaming.appmaster.AppMaster.{AllocateResourceTimeOut, LookupTaskActorRef, TaskActorRef}
import org.apache.gearpump.streaming.appmaster.ClockService.ChangeToNewDAG
import org.apache.gearpump.streaming.appmaster.DagManager.{GetLatestDAG, GetTaskLaunchData, LatestDAG, NewDAGDeployed, TaskLaunchData, WatchChange}
import org.apache.gearpump.streaming.appmaster.ExecutorManager._
import org.apache.gearpump.streaming.appmaster.TaskManager._
import org.apache.gearpump.streaming.appmaster.TaskRegistry.{Accept, TaskLocation}
import org.apache.gearpump.streaming.executor.Executor.RestartTasks
import org.apache.gearpump.streaming.executor.ExecutorRestartPolicy
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.streaming.util.ActorPathUtil
import org.apache.gearpump.streaming.{DAG, ExecutorId, ProcessorId}
import org.apache.gearpump.util.{Constants, LogUtil}
import org.slf4j.Logger

import scala.concurrent.Future
import scala.concurrent.duration._

private[appmaster] class TaskManager(
    appId: Int,
    dagManager: ActorRef,
    subDAGManager: SubDAGManager,
    executorManager: ActorRef,
    clockService: ActorRef,
    appMaster: ActorRef,
    appName: String)
  extends Actor with FSM[State, StateData] {

  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)
  val systemConfig = context.system.settings.config

  private val ids = new SessionIdFactory()

  private val executorRestartPolicy = new ExecutorRestartPolicy(maxNrOfRetries = 5, withinTimeRange = 20 seconds)
  implicit val timeout = Constants.FUTURE_TIMEOUT
  implicit val actorSystem = context.system
  import context.dispatcher

  self ! Initialize
  startWith(Uninitialized, null)

  dagManager ! WatchChange(watcher = self)
  executorManager ! SetTaskManager(self)

  import org.apache.gearpump.TimeStamp

  private def getMinClock: Future[TimeStamp] = {
    (clockService ? GetLatestMinClock).asInstanceOf[Future[LatestMinClock]].map(_.clock)
  }

  private def getStartClock: Future[TimeStamp] = {
    (clockService ? GetStartClock).asInstanceOf[Future[StartClock]].map(_.clock)
  }

  private var startClock: Future[TimeStamp] = getStartClock

  private val startTasks: StateFunction = {
    case Event(executor: ExecutorStarted, state) =>
      import executor.{boundedJar, workerId, executorId, resource}
      val taskIds = subDAGManager.scheduleTask(boundedJar.get, workerId, executorId, resource)
      LOG.info(s"Executor $executor has been started, start to schedule tasks: ${taskIds.mkString(",")}")

      taskIds.groupBy(_.processorId).foreach { pair =>
        val (processorId, tasks) = pair
        dagManager ! GetTaskLaunchData(state.dag.version, processorId, StartTasksOnExecutor(executor.executorId, tasks))
      }
      stay
    case Event(StartExecutorsTimeOut, _) =>
      appMaster ! AllocateResourceTimeOut
      stay
    case Event(ExecutorStopped(executorId), _) =>
      if(executorRestartPolicy.allowRestartExecutor(executorId)) {
        val resourceRequestDetail = subDAGManager.executorFailed(executorId)
        executorManager ! StartExecutors(resourceRequestDetail.requests, resourceRequestDetail.jar)
      } else {
        LOG.error(s"Executor restarted too many times")
      }
      stay

    case Event(TaskLaunchData(processorDescription, subscribers, command), state) =>
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
      stay
    case Event(TasksLaunched, state) =>
      // We will track all launched task by message RegisterTask
      stay
    case Event(TasksChanged, state) =>
      // We will track all changed task by message TaskChanged.
      stay
    case Event(RegisterTask(taskId, executorId, host), state) =>
      val client = sender
      val register = state.taskRegistry
      val status = register.registerTask(taskId, TaskLocation(executorId, host))
      if (status == Accept) {
        LOG.info(s"RegisterTask($taskId) TaskLocation: $host, Executor: $executorId")
        val sessionId = ids.newSessionId
        startClock.map(client ! Start(_, sessionId))
        checkApplicationReady(state)
      } else {
        sender ! TaskRejected
        stay
      }

    case Event(TaskChanged(taskId, dagVersion), state) =>
      state.taskChangeRegistry.taskChanged(taskId)
      checkApplicationReady(state)
      
    case Event(CheckApplicationReady, state) =>
      checkApplicationReady(state)
  }

  private def checkApplicationReady(state: StateData): State = {
    import state.{taskChangeRegistry, taskRegistry}
    if (taskRegistry.isAllTasksRegistered && taskChangeRegistry.allTaskChanged) {
      LOG.info(s"All tasks have been launched; send Task locations to all executors")
      val taskLocations = taskRegistry.getTaskLocations
      (clockService ? ChangeToNewDAG(state.dag)).map { _ =>
        executorManager ! BroadCast(taskLocations)
      }

      val recoverState = StateData(state.dag, new TaskRegistry(appId, state.dag.tasks))
      goto(ApplicationReady) using state.copy(recoverState = recoverState)
    } else {
      stay
    }
  }

  private val onClockEvent: StateFunction = {
    case Event(clock: ClockEvent, _) =>
      clockService forward clock
      stay
  }

  private val ignoreClockEvent: StateFunction = {
    case Event(clock: ClockEvent, _) =>
      stay
  }

  private val onClientQuery: StateFunction = {
    case Event(LookupTaskActorRef(taskId), state) =>
      val executorId = state.taskRegistry.getExecutorId(taskId)
      val requestor = sender
      executorId.map { executorId =>
        val taskPath = ActorPathUtil.taskActorPath(appMaster, executorId, taskId)
        context.actorSelection(taskPath).resolveOne(3 seconds).map { taskActorRef =>
          requestor ! TaskActorRef(taskActorRef)
        }
      }
      stay

    case Event(GetTaskList, state) =>
      val register = state.taskRegistry
      val taskList = register.getTaskLocations.locations.flatMap { pair =>
        val (hostPort, taskSet) = pair
        taskSet.map{ taskId =>
          (taskId, register.getExecutorId(taskId).getOrElse(-1))
        }
      }
      sender ! TaskList(taskList)
      stay
  }

  when (Uninitialized) {
    case Event(Initialize, _) =>
      val state = StateData(DAG.empty().copy(version = -1), new TaskRegistry(appId, List.empty[TaskId]),
        new TaskChangeRegistry(List.empty[TaskId]), null)
      goto (ApplicationReady) using state.copy(recoverState = state)
  }

  when(Recovery)(startTasks orElse ignoreClockEvent)

  private val onMessageLoss: StateFunction = {
    case Event(executorStopped@ExecutorStopped(executorId), state) =>
      if (state.taskRegistry.isTaskRegisteredForExecutor(executorId)) {
        self ! executorStopped
        goto(Recovery) using state.recoverState
      } else {
        stay
      }

    case Event(MessageLoss(executorId, cause), state) =>
      if (state.taskRegistry.isTaskRegisteredForExecutor(executorId)) {
        goto(Recovery) using state.recoverState
      } else {
        stay
      }
    case Event(reply: ReplayFromTimestampWindowTrailingEdge, state) =>
      goto(Recovery) using state.recoverState
  }

  private val onNewDAG: StateFunction = {
    case Event(LatestDAG(newDag), state: StateData) =>
      import state.taskRegistry
      if (newDag.version == state.dag.version) {
        stay
      } else {
        val dagDiff = migrate(state.dag, newDag)
        subDAGManager.setDag(newDag)
        val resourceRequestsDetails = subDAGManager.getRequestDetails()
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
        val dynamicTaskRegistration = new TaskRegistry(appId, newDag.tasks, startedTasks)
        val recoverState = state.copy(taskRegistry = new TaskRegistry(appId, state.dag.tasks))

        val nextState = StateData(newDag, dynamicTaskRegistration, taskChangeRegistry , recoverState)
        goto(DynamicDAG) using nextState
      }
  }

  when(ApplicationReady)(onMessageLoss orElse onClientQuery orElse onNewDAG orElse onClockEvent)

  when(DynamicDAG)(onMessageLoss orElse startTasks orElse onClockEvent orElse onClientQuery)

  onTransition {
    case _ -> ApplicationReady =>
      val newDagVersion = nextStateData.dag.version
      executorManager ! nextStateData.taskRegistry.usedResource
      dagManager ! NewDAGDeployed(newDagVersion)
      dagManager ! GetLatestDAG
      LOG.info(s"goto state ApplicationReady(dag = ${newDagVersion})...")

    case _ -> Recovery =>
      val recoverDagVersion = nextStateData.dag.version
      executorManager ! BroadCast(RestartTasks(recoverDagVersion))
      subDAGManager.setDag(nextStateData.dag)
      self ! CheckApplicationReady

      // Use new Start Clock so that we recover at timepoint we fails.
      startClock = getStartClock
      LOG.info(s"goto state Recovery(recoverDag = $recoverDagVersion)...")

    case _ -> DynamicDAG =>
      LOG.info(s"goto state DynamicDAG from dag: ${stateData.dag.version} to ${nextStateData.dag.version}...")
  }
}

private [appmaster] object TaskManager {

  /**
   * State machine states
   */
  sealed trait State
  case object Uninitialized extends State
  case object ApplicationReady extends State
  case object Recovery extends State
  case object DynamicDAG extends State

  case class StateData(
      dag: DAG, taskRegistry: TaskRegistry,
      taskChangeRegistry: TaskChangeRegistry = TaskChangeRegistry.empty,
      recoverState: StateData = null)

  case object GetTaskList

  case class TaskList(tasks: Map[TaskId, ExecutorId])

  case class StartTasksOnExecutor(executorId: Int, tasks: List[TaskId])
  case class ChangeTasksOnExecutor(executorId: Int, tasks: List[TaskId])

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

  case object Initialize
  case object CheckApplicationReady

  case class DAGDiff(addedProcessors: List[ProcessorId], modifiedProcessors: List[ProcessorId], impactedUpstream: List[ProcessorId])

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