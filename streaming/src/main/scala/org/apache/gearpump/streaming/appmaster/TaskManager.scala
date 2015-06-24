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
import org.apache.gearpump.cluster.MasterToAppMaster.ReplayFromTimestampWindowTrailingEdge
import org.apache.gearpump.streaming.AppMasterToExecutor.{TaskChanged, ChangeTasks, LaunchTasks, StartClock}
import org.apache.gearpump.streaming.appmaster.DagManager.{DAGScheduled, GetTaskLaunchData, WatchChange, TaskLaunchData, LatestDAG, GetLatestDAG}
import org.apache.gearpump.streaming.appmaster.TaskRegistry.{TaskLocation}
import org.apache.gearpump.streaming.executor.{ExecutorRestartPolicy, Executor}
import Executor.RestartTasks
import org.apache.gearpump.streaming.ExecutorToAppMaster.RegisterTask
import org.apache.gearpump.streaming.appmaster.AppMaster.{TaskActorRef, LookupTaskActorRef, AllocateResourceTimeOut}
import org.apache.gearpump.streaming.appmaster.ExecutorManager._
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.streaming.{TaskIndex, ExecutorId, DAG}
import org.apache.gearpump.streaming.util.ActorPathUtil
import org.apache.gearpump.util.{Constants, LogUtil}
import org.slf4j.Logger

import scala.concurrent.Future
import scala.concurrent.duration._
import org.apache.gearpump.streaming.appmaster.TaskManager._

private[appmaster] class TaskManager(
    appId: Int,
    dagManager: ActorRef,
    taskScheduler: TaskScheduler,
    executorManager: ActorRef,
    clockService: ActorRef,
    appMaster: ActorRef,
    appName: String)
  extends Actor with FSM[State, StateData] {

  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)
  val systemConfig = context.system.settings.config

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

  private val startTasks: StateFunction = {
    case Event(executor: ExecutorStarted, state) =>
      LOG.info("Executor has been started, start to launch tasks")

      val taskIds = taskScheduler.resourceAllocated(executor.workerId, executor.executorId, executor.resource)

      LOG.info(s"Going to scheudle ${taskIds} tasks")

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
        val newResourceRequests = taskScheduler.executorFailed(executorId)
        executorManager ! StartExecutors(newResourceRequests)
      } else {
        LOG.error(s"Executor restarted too many times")
      }
      stay

    case Event(TaskLaunchData(processorDescription, subscribers, command), _) =>
      command match {
        case StartTasksOnExecutor(executorId, tasks) =>
          LOG.info("Sending Launch Task to executor: " + executorId + ", tasks: " + tasks)
          val launchTasks = LaunchTasks(tasks, processorDescription, subscribers)
          executorManager ! UniCast(executorId, launchTasks)
          tasks.foreach(executorRestartPolicy.addTaskToExecutor(executorId, _))
        case ChangeTasksOnExecutor(executorId, tasks) =>
          LOG.info("change Task on executor: " + executorId + ", tasks: " + tasks)
          val changeTasks = ChangeTasks(tasks, processorDescription.life, subscribers)
          executorManager ! UniCast(executorId, changeTasks)
        case other =>
          LOG.error(s"severe error! we expect ExecutorStarted but get ${other.getClass.toString}")
      }
      stay
    case Event(RegisterTask(taskId, executorId, host), state) =>
      val client = sender
      getMinClock.map(client ! StartClock(_))
      val register = state.taskRegistry
      register.registerTask(taskId, TaskLocation(executorId, host))
      checkApplicationReady(state)

    case Event(TaskChanged(taskId), state) =>
      state.taskChangeRegistry.taskChanged(taskId)
      checkApplicationReady(state)
      
    case Event(CheckApplicationReady, state) =>
      checkApplicationReady(state)
  }

  private def checkApplicationReady(state: StateData): State = {
    import state.{taskRegistry, taskChangeRegistry}
    if (taskRegistry.isAllTasksRegistered && taskChangeRegistry.allTaskChanged) {
      LOG.info(s"Sending Task locations to executors")
      taskRegistry.getTaskLocations.locations.toArray.foreach { pair =>
        val (hostPort, taskSet) = pair
        taskSet.foreach { taskId =>
          val executorId = taskRegistry.getExecutorId(taskId)
          LOG.info(s"TaskLocation: $hostPort task: $taskId, executor: $executorId")
        }
      }

      executorManager ! BroadCast(taskRegistry.getTaskLocations)

      val recoverState = StateData(state.dag, new TaskRegistry(appId, state.dag.tasks))
      goto(ApplicationReady) using state.copy(recoverState = recoverState)
    } else {
      stay
    }
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

  when(Recovery)(startTasks)

  private val onMessageLoss: StateFunction = {
    case Event(executorStopped@ExecutorStopped(executorId), state) =>
      self ! executorStopped
      goto(Recovery) using state.recoverState
    case Event(MessageLoss, state) =>
      goto(Recovery) using state.recoverState
    case Event(reply: ReplayFromTimestampWindowTrailingEdge, state) =>
      goto(Recovery) using state.recoverState
  }

  private val onNewDAG: StateFunction = {
    case Event(LatestDAG(newDag), state: StateData) =>
      import state.taskRegistry
      if (newDag.version == state.dag.version) {
        stay
      } else {

        val dagDiff = newDag diff state.dag

        taskScheduler.setDAG(newDag)
        val resourceRequests = taskScheduler.getResourceRequests()
        executorManager ! StartExecutors(resourceRequests)

        var modifiedTasks = List.empty[TaskId]
        for (processorId <- dagDiff.modifiedProcessors) {
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

  when(ApplicationReady)(onMessageLoss orElse onClientQuery orElse onNewDAG)

  when(DynamicDAG)(onMessageLoss orElse startTasks)

  onTransition {
    case _ -> ApplicationReady =>
      LOG.info("goto state ApplicationReady...")

      dagManager ! GetLatestDAG
      dagManager ! DAGScheduled(nextStateData.dag.version)
    case _ -> Recovery =>
      LOG.info("goto state Recovery...")
      executorManager ! BroadCast(RestartTasks)
      taskScheduler.setDAG(nextStateData.dag)
      self ! CheckApplicationReady
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

  case object MessageLoss

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

  object Initialize
  object CheckApplicationReady
}