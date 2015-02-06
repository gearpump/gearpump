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
package org.apache.gearpump.streaming

import com.typesafe.config.Config
import org.apache.gearpump.cluster.scheduler.{Relaxation, Resource, ResourceRequest}
import org.apache.gearpump.streaming.TaskLocator.{Locality, NonLocality, WorkerLocality}
import org.apache.gearpump.streaming.TaskSchedulerImpl.TaskLaunchData
import org.apache.gearpump.streaming.task.TaskId
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.collection.mutable

trait TaskScheduler {
  def dagUpdated(dag: DAG): Array[ResourceRequest]
  def resourceAllocated(workerId : Int, executorId: Int) : Option[TaskLaunchData]
  def resourceFailed(executorId: Int) : Array[ResourceRequest]
}

class TaskSchedulerImpl(appId : Int, config: Config)  extends TaskScheduler {
  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)

  private var taskMap = Map.empty[TaskId, TaskLaunchData]
  private var taskQueues = Map.empty[Locality, mutable.Queue[TaskLaunchData]]
  private val taskLocator = new TaskLocator(config)

  private var executorToTaskIds = Map.empty[Int, Set[TaskId]]

  def dagUpdated(dag: DAG): Array[ResourceRequest] ={
    dag.tasks.foreach { params =>
      val (taskGroupId, taskDescription) = params
      0.until(taskDescription.parallelism).map((taskIndex: Int) => {
        val taskId = TaskId(taskGroupId, taskIndex)
        val locality = taskLocator.locateTask(taskDescription)
        val taskLaunchData = TaskLaunchData(taskId, taskDescription, dag.subGraph(taskGroupId))
        taskMap += (taskId -> taskLaunchData)
        addTask(taskLaunchData, locality)
      })
    }
    val nonLocalityQueue = taskQueues.getOrElse(NonLocality, mutable.Queue.empty[TaskLaunchData])
    //reorder the tasks to distributing fairly on workers
    val taskQueue = nonLocalityQueue.sortBy(_.taskId.index)
    taskQueues += (NonLocality -> taskQueue)

    fetchResourceRequests(false)
  }

  private def fetchResourceRequests(fromOneWorker: Boolean = false): Array[ResourceRequest] ={
    var resourceRequests = Array.empty[ResourceRequest]
    taskQueues.foreach(params => {
      val (locality, tasks) = params
      LOG.debug(s"locality : $locality, task queue size : ${tasks.size}")
      locality match {
        case locality: WorkerLocality =>
          if(tasks.size > 0) {
            resourceRequests = resourceRequests :+ ResourceRequest(Resource(tasks.size), locality.workerId, relaxation = Relaxation.SPECIFICWORKER)
          }
        case _ =>
          if(tasks.size > 0){
            if(fromOneWorker){
              resourceRequests = resourceRequests :+ ResourceRequest(Resource(tasks.size), relaxation = Relaxation.ONEWORKER)
            } else {
              resourceRequests = resourceRequests :+ ResourceRequest(Resource(tasks.size))
            }
          }
      }
    })
    resourceRequests
  }

  def resourceAllocated(workerId : Int, executorId: Int) : Option[TaskLaunchData] = {
    val locality = WorkerLocality(workerId)
    val taskQueue = taskQueues.getOrElse(locality, mutable.Queue.empty[TaskLaunchData])
    val task =
      if(taskQueue.nonEmpty) {
      taskQueue.dequeue()
      } else {
        val nonLocalityQueue = taskQueues.getOrElse(NonLocality, mutable.Queue.empty[TaskLaunchData])
        if(nonLocalityQueue.nonEmpty){
          nonLocalityQueue.dequeue()
        } else {
          null
        }
      }

    if (task != null) {
      var taskSetForExecutorId = executorToTaskIds.getOrElse(executorId, Set.empty[TaskId])
      taskSetForExecutorId += task.taskId
      executorToTaskIds += executorId -> taskSetForExecutorId
    }
    Option(task)
  }

  def resourceFailed(executorId: Int) : Array[ResourceRequest] = {
    val tasks = executorToTaskIds.get(executorId)
    executorToTaskIds = executorToTaskIds - executorId

    tasks match {
      case Some(tasks) =>
        tasks.foreach { taskId =>
          val task = taskMap(taskId)
          addTask(task)
        }
      case None =>
    }

    fetchResourceRequests(true)
  }

  private def addTask(taskLaunchData : TaskLaunchData, locality : Locality = NonLocality): Unit = {
    val taskQueue = taskQueues.getOrElse(locality, mutable.Queue.empty[TaskLaunchData])
    taskQueue.enqueue(taskLaunchData)
    taskQueues += (locality -> taskQueue)
  }
}

object TaskSchedulerImpl {
  case class TaskLaunchData(taskId: TaskId, taskDescription : TaskDescription, dag : DAG)
}
