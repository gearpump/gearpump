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
package org.apache.gearpump.streaming.appmaster

import org.apache.gearpump.cluster.scheduler.{Relaxation, Resource, ResourceRequest}
import org.apache.gearpump.streaming.appmaster.ScheduleUsingUserConfig.{WorkerLocality, NonLocality, Locality}
import org.apache.gearpump.streaming.{DAG, TaskDescription}
import org.apache.gearpump.streaming.appmaster.TaskSchedulerImpl.TaskLaunchData
import org.apache.gearpump.streaming.task.TaskId
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.collection.mutable

/**
 * This schedules tasks to run for new allocated resources.
 */
trait TaskScheduler {

  /**
   * This notify the scheduler that the task DAG is created.
   * @param dag task dag
   */
  def setTaskDAG(dag: DAG): Unit

  /**
   * Get the resource requirements for all unscheduled tasks.
   */
  def getResourceRequests(): Array[ResourceRequest]

  /**
   * This notify the scheduler that a resource slot on {workerId} and {executorId} is allocated, and
   * expect a task to run in return.
   * Task locality should be considered when deciding whether to offer a task on target {worker}
   * and {executor}
   *
   * @param workerId which worker this resource is located.
   * @param executorId which executorId this resource belongs to.
   * @return
   */
  def resourceAllocated(workerId : Int, executorId: Int) : Option[TaskLaunchData]

  /**
   * This notify the scheduler that {executorId} is failed, and expect a set of
   * ResourceRequest for all failed tasks on that executor.
   *
   * @param executorId executor that failed
   * @return
   */
  def executorFailed(executorId: Int) : Array[ResourceRequest]

  /**
   * This notify the scheduler to update the task schedule policy, the policy will determinate
   * the task location to launch.
   *
   * @param taskSchedulePolicy task schedule policy that will be used
   */
  def updateTaskSchedulePolicy(taskSchedulePolicy: TaskSchedulePolicy): Unit
}

class TaskSchedulerImpl(appId : Int) extends TaskScheduler {
  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)

  private var tasks = Map.empty[TaskId, TaskLaunchData]

  // maintain a list to tasks to be scheduled
  private var tasksToBeScheduled = Map.empty[Locality, mutable.Queue[TaskLaunchData]]

  private var taskSchedulePolicy: TaskSchedulePolicy = null

  private var executorToTaskIds = Map.empty[Int, Set[TaskId]]

  def setTaskDAG(dag: DAG): Unit = {
    dag.processors.foreach { params =>
      val (taskGroupId, taskDescription) = params
      0.until(taskDescription.parallelism).map((taskIndex: Int) => {
        val taskId = TaskId(taskGroupId, taskIndex)
        val locality = taskSchedulePolicy.scheduleTask(taskDescription)
        val taskLaunchData = TaskLaunchData(taskId, taskDescription, dag.subGraph(taskGroupId))
        tasks += (taskId -> taskLaunchData)
        scheduleTaskLater(taskLaunchData, locality)
      })
    }
    val nonLocalityQueue = tasksToBeScheduled.getOrElse(NonLocality, mutable.Queue.empty[TaskLaunchData])
    //reorder the tasks to distributing fairly on workers
    val taskQueue = nonLocalityQueue.sortBy(_.taskId.index)
    tasksToBeScheduled += (NonLocality -> taskQueue)
  }

  def getResourceRequests(): Array[ResourceRequest] ={
    fetchResourceRequests(fromOneWorker = false)
  }

  import Relaxation._
  private def fetchResourceRequests(fromOneWorker: Boolean = false): Array[ResourceRequest] ={
    var resourceRequests = Array.empty[ResourceRequest]
    tasksToBeScheduled.foreach(taskWithLocality => {
      val (locality, tasks) = taskWithLocality
      LOG.debug(s"locality : $locality, task queue size : ${tasks.size}")

      if (tasks.size > 0) {
        locality match {
          case locality: WorkerLocality =>
            resourceRequests = resourceRequests :+
              ResourceRequest(Resource(tasks.size), locality.workerId, relaxation = SPECIFICWORKER)
          case _ if fromOneWorker =>
              resourceRequests = resourceRequests :+ ResourceRequest(Resource(tasks.size), relaxation = ONEWORKER)
          case _ =>
              resourceRequests = resourceRequests :+ ResourceRequest(Resource(tasks.size))
        }
      }
    })
    resourceRequests
  }

  def resourceAllocated(workerId : Int, executorId: Int) : Option[TaskLaunchData] = {
    val locality = WorkerLocality(workerId)

    // first try to schedule with worker locality. If not found, search tasks without locality
    val task = scheduleTaskWithLocality(locality).orElse(scheduleTaskWithLocality(NonLocality))
    task.map(task => addTaskToExecutor(executorId, task.taskId))
    task
  }

  private def addTaskToExecutor(executorId: Int, task: TaskId): Unit = {
    var taskSetForExecutorId = executorToTaskIds.getOrElse(executorId, Set.empty[TaskId])
    taskSetForExecutorId += task
    executorToTaskIds += executorId -> taskSetForExecutorId
  }

  private def scheduleTaskWithLocality(locality: Locality): Option[TaskLaunchData] = {
    tasksToBeScheduled.get(locality).flatMap { queue =>
      if (queue.isEmpty) {
        None
      } else {
        Some(queue.dequeue())
      }
    }
  }

  def executorFailed(executorId: Int) : Array[ResourceRequest] = {
    val tasksForExecutor = executorToTaskIds.get(executorId)
    executorToTaskIds = executorToTaskIds - executorId

    tasksForExecutor.map{ taskIds =>
      taskIds.foreach { taskId =>
        val task = tasks(taskId)
        //add the failed task back
        scheduleTaskLater(task)
      }
    }

    fetchResourceRequests(fromOneWorker = true)
  }

  private def scheduleTaskLater(taskLaunchData : TaskLaunchData, locality : Locality = NonLocality): Unit = {
    val taskQueue = tasksToBeScheduled.getOrElse(locality, mutable.Queue.empty[TaskLaunchData])
    taskQueue.enqueue(taskLaunchData)
    tasksToBeScheduled += (locality -> taskQueue)
  }

  def updateTaskSchedulePolicy(taskSchedulePolicy: TaskSchedulePolicy): Unit = {
    this.taskSchedulePolicy = taskSchedulePolicy
  }
}

object TaskSchedulerImpl {
  case class TaskLaunchData(taskId: TaskId, taskDescription : TaskDescription, dag : DAG)
}
