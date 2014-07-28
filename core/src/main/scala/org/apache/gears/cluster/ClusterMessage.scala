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
import org.apache.gears.cluster.AppMasterToWorker.LaunchExecutor


/**
 * Cluster Bootup Flow
 */
object WorkerToMaster {
  case class RegisterWorker(workerId: Int)
  case class ResourceUpdate(workerId: Int, slots: Int)
}

object MasterToWorker {
  case object WorkerRegistered
}

/**
 * Application Flow
 */

object ClientToMaster {
  case class SubmitApplication(appMaster: Class[_ <: Actor], config: Configs, appDescription: Application)
  case class ShutdownApplication(appId: Int)
  case object Shutdown
}

object ExecutorToWorker {
  case class RegisterExecutor(appMaster: ActorRef, appId: Int, executorId: Int, slots: Int)
  case class ExecutorFailed(reason: String, appMaster: ActorRef, executorId: Int)
}

object AppMasterToMaster {
  case class RequestResource(appId: Int, slots: Int)
}

object MasterToAppMaster {
  case class Resource(worker: ActorRef, slots: Integer)
  case class ResourceAllocated(resource: Array[Resource])
}

object AppMasterToWorker {
  case class LaunchExecutor(appId: Int, executorId: Int, slots: Int, executor: Props, executorContext: ExecutorContext)
  case class LaunchExecutorOnSystem(appMaster: ActorRef, launch: LaunchExecutor, systemPath: DaemonActorSystem)
}

object WorkerToAppMaster {
  case class ExecutorLaunched(executor: ActorRef, executorId: Int, slots: Int)
  case class ExecutorLaunchFailed(launch: LaunchExecutor, reason: String = null, ex: Throwable = null)
}

object AppMasterToExecutor {
  case class LaunchTask(taskId: TaskId, conf: Map[String, Any], taskDescription: TaskDescription, nextStageTaskId: StageParallism)
  case class TaskLocation(taskId: TaskId, task: ActorRef)
}

object ExecutorToAppMaster {
  sealed trait TaskStatus
  case class TaskLaunched(taskId: TaskId, task: ActorRef)
  case object TaskSuccess
  case class TaskFailed(taskId: TaskId, reason: String = null, ex: Exception = null)
  case class GetTaskLocation(taskId: TaskId)
}