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

import org.apache.gearpump.cluster.scheduler.{Relaxation, Resource, ResourceRequest}
import org.apache.gearpump.streaming.AppMaster.TaskLaunchData
import org.apache.gearpump.streaming.task.TaskId
import org.apache.gearpump.util.{LogUtil, Configs}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class TaskSet(config : Configs, dag : DAG) {
  private val LOG: Logger = LogUtil.getLogger(classOf[TaskSet], app = config.appId)

  private var taskMap = Map.empty[TaskId, TaskLaunchData]
  private var taskQueues = Map.empty[Locality, mutable.Queue[TaskLaunchData]]
  private val taskLocator = new TaskLocator(config)
  var totalTaskCount = 0

  init(dag)

  def fetchResourceRequests(fromOneWorker: Boolean = false): Array[ResourceRequest] ={
    var resourceRequests = Array.empty[ResourceRequest]
    taskQueues.foreach(params => {
      val (locality, tasks) = params
      LOG.debug(s"locality : $locality, task queue size : ${tasks.size}")
      locality match {
        case locality: WorkerLocality =>
          resourceRequests = resourceRequests :+ ResourceRequest(Resource(tasks.size), locality.workerId, relaxation = Relaxation.SPECIFICWORKER)
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

  def hasNotLaunchedTask : Boolean = {
    for(tasks <- taskQueues){
      val (_, taskQueue) = tasks
      if(taskQueue.size > 0)
        return true
    }
    false
  }

  def scheduleTaskOnWorker(workerId : Int) : TaskLaunchData = {
    val locality = WorkerLocality(workerId)
    val taskQueue = taskQueues.getOrElse(locality, mutable.Queue.empty[TaskLaunchData])
    if(taskQueue.nonEmpty) {
      return taskQueue.dequeue()
    } else {
      val nonLocalityQueue = taskQueues.getOrElse(NonLocality, mutable.Queue.empty[TaskLaunchData])
      if(nonLocalityQueue.nonEmpty){
        return nonLocalityQueue.dequeue()
      }
    }
    null
  }

  def taskFailed(tasks : Iterable[TaskId]) : Unit = {
    if (null != tasks) {
      tasks.foreach { taskId =>
        val task = taskMap(taskId)
        addTask(task)
      }
    }
  }

  private def addTask(taskLaunchData : TaskLaunchData, locality : Locality = NonLocality): Unit = {
    val taskQueue = taskQueues.getOrElse(locality, mutable.Queue.empty[TaskLaunchData])
    taskQueue.enqueue(taskLaunchData)
    taskQueues += (locality -> taskQueue)
  }


  private def init(dag : DAG): Unit ={
    dag.tasks.foreach { params =>
      val (taskGroupId, taskDescription) = params
      0.until(taskDescription.parallelism).map((taskIndex: Int) => {
        val taskId = TaskId(taskGroupId, taskIndex)
        val locality = taskLocator.locateTask(taskDescription)
        val taskLaunchData = TaskLaunchData(taskId, taskDescription, dag.subGraph(taskGroupId))
        taskMap += (taskId -> taskLaunchData)
        addTask(taskLaunchData, locality)
        totalTaskCount += 1
      })
    }
    val nonLocalityQueue = taskQueues.getOrElse(NonLocality, mutable.Queue.empty[TaskLaunchData])
    //reorder the tasks to distributing fairly on workers
    val taskQueue = nonLocalityQueue.sortBy(_.taskId.index)
    taskQueues += (NonLocality -> taskQueue)
  }
}
