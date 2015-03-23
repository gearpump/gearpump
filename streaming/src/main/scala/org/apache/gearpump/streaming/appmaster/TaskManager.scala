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
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.streaming.AppMasterToExecutor.{LaunchTask, StartClock}
import org.apache.gearpump.streaming.executor.{ExecutorRestartPolicy, Executor}
import Executor.RestartExecutor
import org.apache.gearpump.streaming.ExecutorToAppMaster.RegisterTask
import org.apache.gearpump.streaming.appmaster.AppMaster.{TaskActorRef, LookupTaskActorRef, AllocateResourceTimeOut}
import org.apache.gearpump.streaming.appmaster.ExecutorManager._
import org.apache.gearpump.streaming.appmaster.TaskSchedulerImpl.TaskLaunchData
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.streaming.DAG
import org.apache.gearpump.streaming.util.ActorPathUtil
import org.apache.gearpump.util.{Constants, LogUtil}
import org.slf4j.Logger

import scala.concurrent.Future
import scala.concurrent.duration._
import TaskManager._

private[appmaster] class TaskManager(
    appId: Int,
    dag: DAG,
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

  startWith(Uninitialized, null)
  self ! DagInit(dag)

  when (Uninitialized) {
    case Event(DagInit(dag), _) =>
      executorManager ! SetTaskManager(self)
      taskScheduler.setTaskDAG(dag)
      val resourceRequests = taskScheduler.getResourceRequests()

      getMinClock.map { clock =>
        LOG.info(s"Current min Clock is $clock, sending request resource to master...")
        executorManager ! StartExecutors(resourceRequests)
      }
      goto (StartApplication) using TaskRegistrationState(new TaskRegistration(appId, dag.taskCount))
  }

  when(StartApplication)(startTasksAndHandleExecutorLoss)

  when(Recovery)(startTasksAndHandleExecutorLoss)

  private def startTasksAndHandleExecutorLoss: StateFunction = {
    case Event(ExecutorStarted(executor, executorId, resource, workerId), state@ TaskRegistrationState(register)) =>
      LOG.info("Executor has been started, start to launch tasks")

      def launchTask(remainResources: Resource): Unit = {
        if (remainResources > Resource.empty) {

          val taskLaunchData = taskScheduler.resourceAllocated(workerId, executorId)

          taskLaunchData match {
            case Some(TaskLaunchData(taskId, taskDescription, dag)) =>
              //Launch task
              LOG.info("Sending Launch Task to executor: " + executor.toString())

              val taskContext = TaskContextData(taskId, executorId, appId, appName, appMaster = appMaster, taskDescription.parallelism, dag)
              executor ! LaunchTask(taskId, taskContext, TaskUtil.loadClass(taskDescription.taskClass))
              executorRestartPolicy.addTaskToExecutor(executorId, taskId)
              //Todo: subtract the actual resource used by task
              val usedResource = Resource(1)
              launchTask(remainResources - usedResource)
            case None =>
              LOG.info("All tasks have been launched")
          }
        }
      }
      launchTask(resource)
      stay using state

    case Event(ExecutorStopped(executorId), state@ TaskRegistrationState( register)) =>
      if(executorRestartPolicy.allowRestartExecutor(executorId)) {
        val newResourceRequests = taskScheduler.executorFailed(executorId)
        executorManager ! StartExecutors(newResourceRequests)
      } else {
        LOG.error(s"Executor restarted to many times")
      }
      stay using state

    case Event(StartExecutorsTimeOut, state: TaskRegistrationState) =>
      appMaster ! AllocateResourceTimeOut
      stay using state

    case Event(RegisterTask(taskId, executorId, host), state@ TaskRegistrationState(register)) =>
      val client = sender
      getMinClock.map(client ! StartClock(_))

      register.registerTask(taskId, executorId, host)
      if (register.isAllTasksRegistered) {
        LOG.info(s"Sending Task locations to executors")
        executorManager ! BroadCast(register.getTaskLocations)
        goto (ApplicationReady) using state
      } else {
        stay using state
      }
  }

  when(ApplicationReady) {
    case Event(clock: UpdateClock, _) =>
      clockService forward clock
      stay

    case Event(GetLatestMinClock, _)=>
      clockService forward GetLatestMinClock
      stay

    case Event(executorStopped @ ExecutorStopped(executorId), _) =>
      //restart all tasks
      executorManager ! BroadCast(RestartExecutor)
      self ! executorStopped
      goto(Recovery) using TaskRegistrationState(new TaskRegistration(appId, dag.taskCount))
    case Event(MessageLoss, _) =>
      //restart all tasks
      executorManager ! BroadCast(RestartExecutor)
      goto(Recovery) using TaskRegistrationState(new TaskRegistration(appId, dag.taskCount))

    case Event(reply: ReplayFromTimestampWindowTrailingEdge, _) =>
      self ! MessageLoss
      stay

    case Event(LookupTaskActorRef(taskId), state@ TaskRegistrationState(register)) =>
      val executorId = register.getExecutorId(taskId)
      val requestor = sender
      executorId.map { executorId =>
        val taskPath = ActorPathUtil.taskActorPath(appMaster, executorId, taskId)
        context.actorSelection(taskPath).resolveOne(3 seconds).map { taskActorRef =>
          requestor ! TaskActorRef(taskActorRef)
        }
      }
      stay
  }

  import org.apache.gearpump.TimeStamp
  private def getMinClock: Future[TimeStamp] = {
    (clockService ? GetLatestMinClock).asInstanceOf[Future[LatestMinClock]].map(_.clock)
  }
}

private [appmaster] object TaskManager {


  /**
   * State machine states
   */
  sealed trait State
  case object Uninitialized extends State
  case object StartApplication extends State
  case object Recovery extends State
  case object ApplicationReady extends State

  /**
   * State machine state data
   */
  sealed trait StateData
  case class TaskRegistrationState(taskRegistration: TaskRegistration) extends StateData


  case class DagInit(dag: DAG)
  case object MessageLoss
}