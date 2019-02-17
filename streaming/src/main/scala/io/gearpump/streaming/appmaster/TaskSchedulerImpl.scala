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
package io.gearpump.streaming.appmaster

import com.typesafe.config.Config
import io.gearpump.cluster.scheduler.{Resource, ResourceRequest}
import io.gearpump.cluster.worker.WorkerId
import io.gearpump.streaming.DAG
import io.gearpump.streaming.appmaster.TaskLocator.{Locality, WorkerLocality}
import io.gearpump.streaming.appmaster.TaskScheduler.{Location, TaskStatus}
import io.gearpump.streaming.task.TaskId
import io.gearpump.util.Constants

/**
 * Schedules tasks to run for new allocated resources. TaskScheduler only schedule tasks that
 * share the same jar. For scheduling for multiple jars, see
 * [[io.gearpump.streaming.appmaster.JarScheduler]].
 */
trait TaskScheduler {

  /**
   * This notify the scheduler that the task DAG is created.
   * @param dag task dag
   */
  def setDAG(dag: DAG): Unit

  /**
   * Get the resource requirements for all unscheduled tasks.
   */
  def getResourceRequests(): Array[ResourceRequest]

  /**
   * This notifies the scheduler that a resource slot on {workerId} and {executorId} is allocated
   * , and expect a task to be scheduled in return.
   * Task locality should be considered when deciding whether to offer a task on target {worker}
   * and {executor}
   *
   * @param workerId which worker this resource is located.
   * @param executorId which executorId this resource belongs to.
   * @return a list of tasks
   */
  def schedule(workerId: WorkerId, executorId: Int, resource: Resource): List[TaskId]

  /**
   * This notifies the scheduler that {executorId} is failed, and expect a set of
   * ResourceRequest for all failed tasks on that executor.
   *
   * @param executorId executor that failed
   * @return resource requests of the failed executor
   */
  def executorFailed(executorId: Int): Array[ResourceRequest]

  /**
   * Queries the task list that already scheduled on the executor
   *
   * @param executorId executor to query
   * @return a list of tasks
   */
  def scheduledTasks(executorId: Int): List[TaskId]
}

object TaskScheduler {
  case class Location(workerId: WorkerId, executorId: Int)

  class TaskStatus(val taskId: TaskId, val preferLocality: Locality, var allocation: Location)
}

class TaskSchedulerImpl(appName: String, config: Config) extends TaskScheduler {
  private val executorNum = config.getInt(Constants.APPLICATION_EXECUTOR_NUMBER)

  private var tasks = List.empty[TaskStatus]

  // Finds the locality of the tasks
  private val taskLocator = new TaskLocator(appName, config)

  override def setDAG(dag: DAG): Unit = {
    val taskMap = tasks.map(_.taskId).zip(tasks).toMap

    tasks = dag.tasks.sortBy(_.index).map { taskId =>
      val locality = taskLocator.locateTask(taskId)
      taskMap.getOrElse(taskId, new TaskStatus(taskId, locality, allocation = null))
    }
  }

  def getResourceRequests(): Array[ResourceRequest] = {
    fetchResourceRequests()
  }

  import io.gearpump.cluster.scheduler.Relaxation._
  private def fetchResourceRequests(): Array[ResourceRequest] = {
    var workersResourceRequest = Map.empty[WorkerId, Resource]

    tasks.filter(_.allocation == null).foreach { task =>
      task.preferLocality match {
        case WorkerLocality(workerId) =>
          val current = workersResourceRequest.getOrElse(workerId, Resource.empty)
          workersResourceRequest += workerId -> (current + Resource(1))
        case _ =>
          val workerId = WorkerId.unspecified
          val current = workersResourceRequest.getOrElse(workerId, Resource.empty)
          workersResourceRequest += workerId -> (current + Resource(1))
      }
    }

    workersResourceRequest.map { workerIdAndResource =>
      val (workerId, resource) = workerIdAndResource
      if (workerId == WorkerId.unspecified) {
        ResourceRequest(resource, workerId = WorkerId.unspecified, executorNum = executorNum)
      } else {
        ResourceRequest(resource, workerId, relaxation = SPECIFICWORKER)
      }
    }.toArray
  }

  override def schedule(workerId: WorkerId, executorId: Int, resource: Resource): List[TaskId] = {
    var scheduledTasks = List.empty[TaskId]
    val location = Location(workerId, executorId)
    // Schedules tasks for specific worker
    scheduledTasks ++= scheduleTasksForLocality(resource, location,
      _ == WorkerLocality(workerId))

    // Schedules tasks without specific location preference
    scheduledTasks ++= scheduleTasksForLocality(resource - Resource(scheduledTasks.length),
      location, _ => true)
    scheduledTasks
  }

  private def scheduleTasksForLocality(
      resource: Resource, resourceLocation: Location, matcher: Locality => Boolean)
    : List[TaskId] = {
    var scheduledTasks = List.empty[TaskId]
    var index = 0
    var remain = resource.slots
    while (index < tasks.length && remain > 0) {
      val taskStatus = tasks(index)
      if (taskStatus.allocation == null && matcher(taskStatus.preferLocality)) {
        taskStatus.allocation = resourceLocation
        scheduledTasks +:= taskStatus.taskId
        remain -= 1
      }
      index += 1
    }
    scheduledTasks
  }

  override def executorFailed(executorId: Int): Array[ResourceRequest] = {
    val failedTasks = tasks.filter { status =>
      status.allocation != null && status.allocation.executorId == executorId
    }
    // Cleans the location of failed tasks
    failedTasks.foreach(_.allocation = null)

    Array(ResourceRequest(Resource(failedTasks.length),
      workerId = WorkerId.unspecified, relaxation = ONEWORKER))
  }

  override def scheduledTasks(executorId: Int): List[TaskId] = {
    tasks.filter { status =>
      status.allocation != null && status.allocation.executorId == executorId
    }.map(_.taskId)
  }
}