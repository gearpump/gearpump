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

import akka.actor.ActorRef
import org.apache.gearpump.cluster.scheduler.{Resource, ResourceAllocation, ResourceRequest}

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
  case class UpdateResourceFailed(reason : String = null, ex: Throwable = null)
}

/**
 * Application Flow
 */

object ClientToMaster {
  case class SubmitApplication(appDescription: Application, appJar: Option[AppJar], username : String = System.getProperty("user.name"))
  case class ShutdownApplication(appId: Int)
}

object MasterToClient {
  case class SubmitApplicationResult(appId : Try[Int])
  case class ShutdownApplicationResult(appId : Try[Int])
  case class ReplayApplicationResult(appId: Try[Int])
}

trait AppMasterRegisterData

object AppMasterToMaster {
  case class RegisterAppMaster(appMaster: ActorRef, appId: Int, executorId: Int, resource: Resource, registerData : AppMasterRegisterData)
  case class InvalidAppMaster(appId: Int, appMaster: String, reason: Throwable)
  case class RequestResource(appId: Int, request: ResourceRequest)
  case class SaveAppData(appId: Int, key: String, value: Any)
  case class GetAppData(appId: Int, key: String)
}

object MasterToAppMaster {
  case class ResourceAllocated(allocations: Array[ResourceAllocation]){
    override def equals(other: Any): Boolean = {
      other match {
        case that: ResourceAllocated =>
          allocations.sortBy(_.workerId).sameElements(that.allocations.sortBy(_.workerId))
        case _ =>
          false
      }
    }
  }
  case class AppMasterRegistered(appId: Int, master : ActorRef)
  case object ShutdownAppMaster
  case class AppMasterData(appId: Int, appData: AppMasterInfo)
  case class AppMasterDataRequest(appId: Int)
  case class AppMastersData(appMasters: List[AppMasterData])
  case object AppMastersDataRequest
  case class AppMasterDataDetailRequest(appId: Int)
  case class AppMasterDataDetail(appId: Int, appDescription: Application)
  case class ReplayFromTimestampWindowTrailingEdge(appId: Int)
  case class GetAppDataResult(key: String, value: Any)
  case object AppDataReceived
}

object AppMasterToWorker {
  case class LaunchExecutor(appId: Int, executorId: Int, resource: Resource, executorContext: ExecutorContext)
  case class ShutdownExecutor(appId : Int, executorId : Int, reason : String)
}

object WorkerToAppMaster {
  case class ExecutorLaunchRejected(reason: String = null, resource : Resource, ex: Throwable = null)
}

