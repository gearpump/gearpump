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

import akka.actor.{Props, ActorRefFactory, ActorRef, Actor}
import com.typesafe.config.Config
import io.gearpump.TimeStamp
import io.gearpump.streaming.task.TaskId
import io.gearpump.streaming.{ProcessorDescription, DAG}
import io.gearpump.cluster.AppJar
import io.gearpump.cluster.scheduler.{Resource, ResourceRequest}
import io.gearpump.partitioner.PartitionerDescription
import io.gearpump.streaming.appmaster.JarScheduler._
import io.gearpump.util.{Constants, Graph}
import akka.pattern.ask
import scala.concurrent.Future

/**
 *
 * With JarScheduler, we allows a DAG to be partitioned into several
 * parts, with each part use its own jar file.
 *
 */
class JarScheduler(appId : Int, appName: String, config: Config, factory: ActorRefFactory) {
  private val actor: ActorRef = factory.actorOf(Props(new JarSchedulerImpl(appId, appName, config)))
  private implicit val dispatcher = factory.dispatcher
  private implicit val timeout = Constants.FUTURE_TIMEOUT

  def setDag(dag: DAG, startClock: Future[TimeStamp]): Unit = {
    startClock.map {start =>
      val init = Init(dag, start)
      actor ! init
    }
  }

  def getRequestDetails(): Future[Array[ResourceRequestDetail]] = {
    (actor ? GetRequestDetails).asInstanceOf[Future[Array[ResourceRequestDetail]]]
  }

  def scheduleTask(appJar: AppJar, workerId: Int, executorId: Int, resource: Resource): Future[List[TaskId]] = {
    (actor ? ScheduleTask(appJar, workerId, executorId, resource)).asInstanceOf[Future[List[TaskId]]]
  }

  def executorFailed(executorId: Int): Future[Option[ResourceRequestDetail]] = {
    (actor ? ExecutorFailed(executorId)).asInstanceOf[Future[Option[ResourceRequestDetail]]]
  }
}

object JarScheduler{
  case class ResourceRequestDetail(jar: AppJar, requests: Array[ResourceRequest])

  case class Init(dag: DAG, startTime: TimeStamp)
  case object GetRequestDetails

  case class ScheduleTask(appJar: AppJar, workerId: Int, executorId: Int, resource: Resource)

  case class ExecutorFailed(executorId: Int)

  class JarSchedulerImpl(appId : Int, appName: String, config: Config) extends Actor {

    private var taskSchedulers = Map.empty[AppJar, TaskScheduler]
    private var startTime: TimeStamp = 0L
    private var dag: DAG = null

    def receive: Receive = {
      case Init(dag, startTime) =>
        this.dag = dag
        this.startTime = startTime

        val processors = dag.processors.values.groupBy(_.jar)
        taskSchedulers = processors.map { jarAndProcessors =>
          val (jar, processors) = jarAndProcessors
          //Construct the sub DAG
          val graph = Graph.empty[ProcessorDescription, PartitionerDescription]
          processors.foreach{processor =>
            if (startTime < processor.life.death) {
              graph.addVertex(processor)
            }
          }
          val subDag = DAG(graph)
          val taskScheduler = taskSchedulers.getOrElse(jar, new TaskSchedulerImpl(appId, appName, config))
          taskScheduler.setDAG(subDag)
          jar -> taskScheduler
        }

      case GetRequestDetails =>
        val result: Array[ResourceRequestDetail] = taskSchedulers.map { jarAndScheduler =>
          val (jar, scheduler) = jarAndScheduler
          ResourceRequestDetail(jar, scheduler.getResourceRequests())
        }.toArray
        sender ! result
      case ScheduleTask(appJar, workerId, executorId, resource) =>
        val result: List[TaskId] = taskSchedulers.get(appJar).map { scheduler =>
          scheduler.schedule(workerId, executorId, resource)
        }.getOrElse(List.empty)
        sender ! result
      case ExecutorFailed(executorId) =>
        val result: Option[ResourceRequestDetail] = taskSchedulers.
          find(_._2.scheduledTasks(executorId).nonEmpty).map{ jarAndScheduler =>
            ResourceRequestDetail(jarAndScheduler._1, jarAndScheduler._2.executorFailed(executorId))
          }
        sender ! result
    }
  }
}
