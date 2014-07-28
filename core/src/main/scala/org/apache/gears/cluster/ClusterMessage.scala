package org.apache.gears.cluster

/**
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

import akka.actor.{Actor, ActorRef, Props}
import org.apache.gearpump.task.TaskId
import org.apache.gearpump.util.ExecutorLauncher.DaemonActorSystem
import org.apache.gearpump.{StageParallism, TaskDescription}

/**
 * Clustering messages between Master, Worker, AppMaster, Executor, Task, Client
 */
sealed trait ClusterMessage

/**
 * Master to Worker Message
 */
trait MasterToWorkerMsg extends ClusterMessage
case object WorkerRegistered extends MasterToWorkerMsg

/**
 * Worker to Master Message
 */
trait WorkerToMasterMsg extends ClusterMessage

case class RegisterWorker(workerId : Int) extends WorkerToMasterMsg
case class ResourceUpdate(workerId : Int,  slots : Int) extends WorkerToMasterMsg

/**
 * Executor to Worker Message
 */
trait ExecutorToWorker extends ClusterMessage
case class RegisterExecutor(appMaster : ActorRef, appId : Int, executorId : Int, slots: Int)
case class ExecutorFailed(reason : String, appMaster : ActorRef, executorId : Int) extends ExecutorToWorker

/**
 * AppMaster to Worker Message
 */
trait AppMasterToWorker extends ClusterMessage
case class LaunchExecutor(appId : Int, executorId : Int, slots : Int, executor : Props, executorContext : ExecutorContext) extends AppMasterToWorker

case class LaunchExecutorOnSystem(appMaster : ActorRef, launch : LaunchExecutor, systemPath : DaemonActorSystem)

/**
 * Worker to AppMaster Message
 */
trait WorkerToAppMaster extends ClusterMessage
case class ExecutorLaunched(executor : ActorRef, executorId : Int, slots: Int) extends WorkerToAppMaster
case class ExecutorLaunchFailed(launch : LaunchExecutor, reason : String = null, ex : Throwable = null) extends WorkerToAppMaster


/**
 * AppMater to Master
 */
trait AppMasterToMaster extends ClusterMessage
case class RequestResource(appId : Int, slots : Int) extends AppMasterToMaster

/**
 * Master to AppMaster
 */
trait MasterToAppMaster extends ClusterMessage

case class Resource(worker : ActorRef, slots : Integer)

case class ResourceAllocated(resource : Array[Resource])

/**
 * Client to Master
 */
trait ClientToAppMaster extends ClusterMessage
case class SubmitApplication(appMaster : Class[_ <: Actor], config : Configs, appDescription : Application) extends ClientToAppMaster
case class ShutdownApplication(appId : Int) extends ClientToAppMaster
case object Shutdown

/**
 * AppMaster to Executor
 */
trait AppMasterToExecutor extends ClusterMessage
case class LaunchTask(taskId : TaskId, conf : Map[String, Any], taskDescription : TaskDescription, nextStageTaskId : StageParallism) extends AppMasterToExecutor
case class TaskLocation(taskId : TaskId, task : ActorRef)

/**
 * Executor to AppMaster
 */
trait ExecutorToAppMaster extends ClusterMessage

sealed trait TaskStatus

case class TaskLaunched(taskId : TaskId, task : ActorRef) extends ExecutorToAppMaster
case object TaskSuccess extends ExecutorToAppMaster
case class TaskFailed(taskId : TaskId, reason : String = null, ex : Exception = null) extends ExecutorToAppMaster

case class GetTaskLocation(taskId : TaskId)
