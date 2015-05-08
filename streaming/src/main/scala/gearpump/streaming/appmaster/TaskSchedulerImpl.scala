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
package gearpump.streaming.appmaster

import com.typesafe.config.Config
import gearpump.streaming.task.{TaskId, Subscriber}
import gearpump.streaming.{ProcessorDescription, DAG}
import gearpump.cluster.scheduler.{Relaxation, Resource, ResourceRequest}
import TaskLocator.{WorkerLocality, NonLocality, Locality}
import TaskSchedulerImpl.TaskLaunchData
import gearpump.util.LogUtil
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
}

class TaskSchedulerImpl(appId : Int, config: Config)  extends TaskScheduler {
  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)

  private var tasks = Map.empty[TaskId, TaskLaunchData]

  // maintain a list to tasks to be scheduled
  private var tasksToBeScheduled = Map.empty[Locality, mutable.Queue[TaskLaunchData]]

  // find the locality of the tasks
  private val taskLocator = new TaskLocator(config)

  private var executorToTaskIds = Map.empty[Int, Set[TaskId]]

  def setTaskDAG(dag: DAG): Unit = {
    dag.processors.foreach { params =>
      val (processorId, taskDescription) = params

      val subscriptions = Subscriber.of(processorId, dag)
      0.until(taskDescription.parallelism).map((taskIndex: Int) => {
        val taskId = TaskId(processorId, taskIndex)
        val locality = taskLocator.locateTask(taskDescription)
        val taskLaunchData = TaskLaunchData(taskId, taskDescription,
          subscriptions)
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

    tasksForExecutor match {
      case Some(taskIds) =>
        taskIds.foreach { taskId =>
          val task = tasks(taskId)

          //add the failed task back
          scheduleTaskLater(task)
        }
        Array(ResourceRequest(Resource(taskIds.size), relaxation = ONEWORKER))
      case None =>
        Array.empty
    }
  }

  private def scheduleTaskLater(taskLaunchData : TaskLaunchData, locality : Locality = NonLocality): Unit = {
    val taskQueue = tasksToBeScheduled.getOrElse(locality, mutable.Queue.empty[TaskLaunchData])
    taskQueue.enqueue(taskLaunchData)
    tasksToBeScheduled += (locality -> taskQueue)
  }
}

object TaskSchedulerImpl {
  case class TaskLaunchData(taskId: TaskId, taskDescription : ProcessorDescription, subscriptions: List[Subscriber])
}
