package org.apache.gearpump
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

import akka.actor.ActorRef

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

case object RegisterWorker extends WorkerToMasterMsg
case class ResourceUpdate(slots : Int) extends WorkerToMasterMsg

/**
 * Executor to Worker Message
 */
trait ExecutorToWorker extends ClusterMessage
case class RegisterExecutor(appMaster : ActorRef, executorId : Int, slots: Int)
case class ExecutorFailed(reason : String, appMaster : ActorRef, executorId : Int) extends ExecutorToWorker

/**
 * AppMaster to Worker Message
 */
trait AppMasterToWorker extends ClusterMessage
case class LaunchExecutor(appId : Int, executorId : Int, slots : Int, executorContext : ExecutorContext) extends AppMasterToWorker

/**
 * Worker to AppMaster Message
 */
trait WorkerToAppMaster extends ClusterMessage
case class ExecutorLaunched(executor : ActorRef, executorId : Int, slots: Int) extends WorkerToAppMaster
case class ExecutorLaunchFailed(executorId : Int, reason : String = null, ex : Exception = null) extends WorkerToAppMaster


/**
 * AppMater to Master
 */
trait AppMasterToMaster extends ClusterMessage
case class RequestResource(slots : Int) extends AppMasterToMaster

/**
 * Master to AppMaster
 */
trait MasterToAppMaster extends ClusterMessage

case class Resource(worker : ActorRef, slots : Integer)

case class ResourceAllocated(resource : Array[Resource])

/**
 * Client to App Master
 */
trait ClientToAppMaster extends ClusterMessage
case class SubmitApplication(appDescription : AppDescription) extends ClientToAppMaster

/**
 * AppMaster to Executor
 */
trait AppMasterToExecutor extends ClusterMessage
case class LaunchTask(taskId : Int, taskDescription : TaskDescription, nextStageTaskId : Range) extends AppMasterToExecutor
case class TaskLocation(taskId : Int, task : ActorRef)

/**
 * Executor to AppMaster
 */
trait ExecutorToAppMaster extends ClusterMessage

sealed trait TaskStatus

case class TaskLaunched(taskId : Int, task : ActorRef) extends ExecutorToAppMaster
case object TaskSuccess extends ExecutorToAppMaster
case class TaskFailed(taskId : Int, reason : String = null, ex : Exception = null) extends ExecutorToAppMaster

case class GetTaskLocation(taskId : Int)
