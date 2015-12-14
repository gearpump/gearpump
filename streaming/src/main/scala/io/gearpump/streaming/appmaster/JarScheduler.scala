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
package io.gearpump.streaming.appmaster

import com.typesafe.config.Config
import io.gearpump.streaming.task.TaskId
import io.gearpump.streaming.{ProcessorDescription, DAG}
import io.gearpump.cluster.AppJar
import io.gearpump.cluster.scheduler.{Resource, ResourceRequest}
import io.gearpump.partitioner.PartitionerDescription
import JarScheduler.ResourceRequestDetail
import io.gearpump.util.Graph

/**
 *
 * With JarScheduler, we allows a DAG to be partitioned into several
 * parts, with each part use its own jar file.
 *
 * @param appId
 * @param appName
 * @param config
 */
class JarScheduler(appId : Int, appName: String, config: Config) {
  private var taskSchedulers = Map.empty[AppJar, TaskScheduler]
  private var executorAndSchedulers = Map.empty[Int, (AppJar, TaskScheduler)]

  def setDag(dag: DAG): Unit = {
    val processors = dag.processors.values.groupBy(_.jar)
    processors.foreach{ jarAndProcessors =>
      val (jar, processors) = jarAndProcessors
      //Construct the sub DAG
      val graph = Graph.empty[ProcessorDescription, PartitionerDescription]
      processors.foreach(graph.addVertex)
      val subDag = DAG(graph)
      val taskScheduler = taskSchedulers.getOrElse(jar, new TaskSchedulerImpl(appId, appName, config))
      taskScheduler.setDAG(subDag)
      taskSchedulers += jar -> taskScheduler
    }
  }

  def getRequestDetails(): Array[ResourceRequestDetail] = {
    taskSchedulers.map { jarAndScheduler =>
      val (jar, scheduler) = jarAndScheduler
      ResourceRequestDetail(jar, scheduler.getResourceRequests())
    }.toArray
  }

  def scheduleTask(appJar: AppJar, workerId: Int, executorId: Int, resource: Resource): List[TaskId] = {
    taskSchedulers.get(appJar).map { scheduler =>
      executorAndSchedulers += executorId -> (appJar, scheduler)
      scheduler.schedule(workerId, executorId, resource)
    }.getOrElse(List.empty)
  }

  def executorFailed(executorId: Int): ResourceRequestDetail = {
    val (jar, scheduler) = executorAndSchedulers(executorId)
    ResourceRequestDetail(jar, scheduler.executorFailed(executorId))
  }
}

object JarScheduler{
  case class ResourceRequestDetail(jar: AppJar, requests: Array[ResourceRequest])
}
