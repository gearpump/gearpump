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

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import org.apache.gearpump.cluster.AppMasterContext
import org.apache.gearpump.cluster.MasterToAppMaster.ReplayFromTimestampWindowTrailingEdge
import org.apache.gearpump.cluster.scheduler.{Resource, ResourceAllocation}
import org.apache.gearpump.streaming.AppMasterToExecutor.{LaunchTask, StartClock}
import org.apache.gearpump.streaming.Executor.RestartExecutor
import org.apache.gearpump.streaming.ExecutorToAppMaster.RegisterTask
import org.apache.gearpump.streaming.TaskSet.TaskLaunchData
import org.apache.gearpump.streaming.appmaster.AppMaster.AllocateResourceTimeOut
import org.apache.gearpump.streaming.appmaster.ExecutorManager.{SetTaskManager, StartExecutors}
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.streaming.{DAG, TaskSet}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.{ActorUtil, Constants, LogUtil}
import org.slf4j.Logger

import scala.concurrent.Future

private[appmaster] class TaskManager(
    appContext: AppMasterContext,
    dag: DAG,
    clockService: ActorRef,
    executorManager: ActorRef,
    appMaster: ActorRef,
    appName: String)
  extends Actor {

  import appContext.appId
private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)

  implicit val timeout = Constants.FUTURE_TIMEOUT
  implicit val actorSystem = context.system

  import context.dispatcher
  private var taskLocations = Map.empty[HostPort, Set[TaskId]]
  private var executorToTaskIds = Map.empty[Int, Set[TaskId]]
  private var startedTasks = Set.empty[TaskId]

  private val taskSet = new TaskSet(appId, dag)
  //When the AppMaster trying to replay, the replay command should not be handled again.
  private var restarting = true

  executorManager ! SetTaskManager(self)
  LOG.info("Sending request resource to master...")
  val resourceRequests = taskSet.fetchResourceRequests()
  executorManager ! StartExecutors(resourceRequests)

  def receive = executorMessageHandler orElse taskMessageHandler orElse recoveryHandler

  //wait for executors, and then start task
import org.apache.gearpump.streaming.appmaster.ExecutorManager._
  def executorMessageHandler: Receive = {
    case ExecutorStarted(executor, executorId, resource, workerId) =>
      LOG.info("Executor has been started, start to launch tasks")

      def launchTask(remainResources: Resource): Unit = {
        if ((remainResources > Resource.empty) && taskSet.hasNotLaunchedTask) {
          val TaskLaunchData(taskId, taskDescription, dag) = taskSet.scheduleTaskOnWorker(workerId)
          //Launch task
          LOG.info("Sending Launch Task to executor: " + executor.toString())

          val taskContext = TaskContext(taskId, executorId, appId, appName, appMaster = appMaster, taskDescription.parallelism, dag)
          executor ! LaunchTask(taskId, taskContext, ActorUtil.loadClass(taskDescription.taskClass))
          //Todo: subtract the actual resource used by task
          val usedResource = Resource(1)
          launchTask(remainResources - usedResource)
        }
      }
      launchTask(resource)

    case ExecutorStopped(executorId) =>
      val tasks = executorToTaskIds.get(executorId)
      tasks match {
        case Some(tasks) =>
          taskSet.taskFailed(tasks)
          appMaster ! ReplayFromTimestampWindowTrailingEdge
        case None =>
          LOG.error(s"executor $executorId is stopped, but there is no tasks started on it yet")
      }

    case StartExecutorsTimeOut =>
      appMaster ! AllocateResourceTimeOut
  }

  def taskMessageHandler: Receive = {
    case RegisterTask(taskId, executorId, host) =>
      LOG.info(s"Task $taskId has been Launched for app $appId")

      var taskIds = taskLocations.getOrElse(host, Set.empty[TaskId])
      taskIds += taskId
      taskLocations += host -> taskIds

      var taskSetForExecutorId = executorToTaskIds.getOrElse(executorId, Set.empty[TaskId])
      taskSetForExecutorId += taskId
      executorToTaskIds += executorId -> taskSetForExecutorId

      startedTasks += taskId

      LOG.info(s" started task size: ${startedTasks.size}, taskQueue size: ${taskSet.totalTaskCount}")
      if (startedTasks.size == taskSet.totalTaskCount) {
        this.restarting = false

        LOG.info(s"Sending Task locations to executors")
        executorManager ! BroadCast(TaskLocations(taskLocations))
      }

      val client = sender
      (clockService ? GetLatestMinClock).asInstanceOf[Future[LatestMinClock]].map{ clock =>
        client ! StartClock(clock.clock)
      }
  }

  def recoveryHandler: Receive = {
    case ReplayFromTimestampWindowTrailingEdge =>
      if(!restarting){
        (clockService ? GetLatestMinClock).asInstanceOf[Future[LatestMinClock]].map { clock =>
          val startClock = clock.clock
          taskLocations = taskLocations.empty
          startedTasks = startedTasks.empty

          if(taskSet.hasNotLaunchedTask){
            val resourceRequests = taskSet.fetchResourceRequests(true)
            executorManager ! StartExecutors(resourceRequests)
          }
          executorManager ! BroadCast(RestartExecutor)
        }
        this.restarting = true
      }
  }

  private def enoughResourcesAllocated(allocations: Array[ResourceAllocation]): Boolean = {
    val totalAllocated = allocations.foldLeft(Resource.empty){ (totalResource, resourceAllocation) =>
      totalResource + resourceAllocation.resource
    }
    LOG.info(s"AppMaster $appId received resource $totalAllocated, ${taskSet.size} tasks remain to be launched")
    totalAllocated.slots == taskSet.size
  }
}