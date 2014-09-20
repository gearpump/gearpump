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

package org.apache.gearpump.cluster

import akka.actor.{Actor, ActorRef}
import org.apache.gearpump.scheduler.{Allocation, Resource}
import org.apache.gearpump.util.Configs

import scala.collection.mutable.HashMap
import scala.util.Try

/**
 * Cluster Bootup Flow
 */
object WorkerToMaster {
  case object RegisterNewWorker
  case class RegisterWorker(workerId: Int)
  case class ResourceUpdate(workerId: Int, resource: Resource)
}

object MasterToWorker {
  case class WorkerRegistered(workerId : Int)
}

/**
 * Application Flow
 */

object ClientToMaster {
  case class SubmitApplication(appMaster: Class[_ <: Actor], config: Configs, appDescription: Application)
  case class ShutdownApplication(appId: Int)
}

object MasterToClient {
  case class SubmitApplicationResult(appId : Try[Int])
  case class ShutdownApplicationResult(appId : Try[Int])
}

trait AppMasterRegisterData

object AppMasterToMaster {
  case class RegisterAppMaster(appMaster: ActorRef, appId: Int, executorId: Int, resources: Resource, registerData : AppMasterRegisterData)
  case class RequestResource(appId: Int, resource : Resource)
  case object GetAllWorkers
}

object MasterToAppMaster {
  case class ResourceAllocated(allocations: Array[Allocation])
  case class AppMasterRegistered(appId: Int, master : ActorRef)
  case object ShutdownAppMaster
  case class WorkerList(list : HashMap[ActorRef, Int])
}

object AppMasterToWorker {
  case class LaunchExecutor(appId: Int, executorId: Int, resource: Resource, executorContext: ExecutorContext)
  case class ShutdownExecutor(appId : Int, executorId : Int, reason : String)
}

object WorkerToAppMaster {
  case class ExecutorLaunchRejected(reason: String = null, ex: Throwable = null)
}