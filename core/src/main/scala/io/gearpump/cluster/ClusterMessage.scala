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

package io.gearpump.cluster

import akka.actor.ActorRef
import com.typesafe.config.Config
import io.gearpump.{WorkerId, TimeStamp}
import io.gearpump.cluster.MasterToAppMaster.AppMasterStatus
import io.gearpump.cluster.master.MasterSummary
import io.gearpump.cluster.scheduler.{Resource, ResourceAllocation, ResourceRequest}
import io.gearpump.cluster.worker.WorkerSummary
import io.gearpump.metrics.Metrics.MetricType

import scala.util.Try

/**
 * Application Flow
 */

object ClientToMaster {
  case object AddMaster
  case class AddWorker(count: Int)
  case class RemoveMaster(masterContainerId: String)
  case class RemoveWorker(workerContainerId: String)
  case class CommandResult(success: Boolean, exception: String = null) {
    override def toString: String = {
      val tag = getClass.getSimpleName
      if (success) {
        s"$tag(success)"
      } else {
        s"$tag(failure, $exception)"
      }
    }
  }
  case class SubmitApplication(appDescription: AppDescription, appJar: Option[AppJar], username : String = System.getProperty("user.name"))
  case class RestartApplication(appId: Int)
  case class ShutdownApplication(appId: Int)
  case class ResolveAppId(appId: Int)

  case class ResolveWorkerId(workerId: WorkerId)

  case object GetJarStoreServer

  case class JarStoreServerAddress(url: String)

  case class QueryAppMasterConfig(appId: Int)

  case class QueryWorkerConfig(workerId: WorkerId)

  case object QueryMasterConfig

  object ReadOption {
    type ReadOption = String

    val Key: String = "readOption"

    val ReadLatest: ReadOption = "readLatest"

    val ReadRecent = "readRecent"

    val ReadHistory = "readHistory"
  }


  case class QueryHistoryMetrics(path: String, readOption: ReadOption.ReadOption = ReadOption.ReadLatest, aggregatorClazz: String = "", options: Map[String, String] = Map.empty[String, String])

  case class GetStallingTasks(appId: Int)

  case class GetLastFailure(appId: Int)
}

object MasterToClient {
  case class SubmitApplicationResult(appId : Try[Int])
  case class SubmitApplicationResultValue(appId: Int)
  case class ShutdownApplicationResult(appId : Try[Int])
  case class ReplayApplicationResult(appId: Try[Int])
  case class ResolveAppIdResult(appMaster: Try[ActorRef])

  case class ResolveWorkerIdResult(worker: Try[ActorRef])

  case class AppMasterConfig(config: Config)

  case class WorkerConfig(config: Config)

  case class MasterConfig(config: Config)

  case class HistoryMetricsItem(time: TimeStamp, value: MetricType)

  case class HistoryMetrics(path: String, metrics: List[HistoryMetricsItem])

  case class LastFailure(time: TimeStamp, error: String)
}

trait AppMasterRegisterData

object AppMasterToMaster {
  case class RegisterAppMaster(appMaster: ActorRef, registerData : AppMasterRegisterData)
  case class InvalidAppMaster(appId: Int, appMaster: String, reason: Throwable)
  case class RequestResource(appId: Int, request: ResourceRequest)

  case class SaveAppData(appId: Int, key: String, value: Any)
  case object AppDataSaved
  case object SaveAppDataFailed

  case class GetAppData(appId: Int, key: String)
  case class GetAppDataResult(key: String, value: Any)

  trait AppMasterSummary {
    def appType: String
    def appId: Int
    def appName: String
    def actorPath: String
    def status: AppMasterStatus
    def startTime: TimeStamp
    def uptime: TimeStamp
    def user: String
  }

  case class GeneralAppMasterSummary(
    appId: Int,
    appType: String = "general",
    appName: String = null,
    actorPath: String = null,
    status: AppMasterStatus = MasterToAppMaster.AppMasterActive,
    startTime: TimeStamp = 0L,
    uptime: TimeStamp = 0L,
    user: String = null)
    extends AppMasterSummary

  case object GetAllWorkers
  case class GetWorkerData(workerId: WorkerId)
  case class WorkerData(workerDescription: WorkerSummary)

  case object GetMasterData
  case class MasterData(masterDescription: MasterSummary)
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
  case class AppMasterRegistered(appId: Int)
  case object ShutdownAppMaster

  type AppMasterStatus = String
  val AppMasterActive: AppMasterStatus = "active"
  val AppMasterInActive: AppMasterStatus = "inactive"
  val AppMasterNonExist: AppMasterStatus = "nonexist"

  sealed trait StreamingType
  case class AppMasterData(status: AppMasterStatus, appId: Int = 0, appName: String = null, appMasterPath: String = null, workerPath: String = null, submissionTime: TimeStamp = 0, startTime: TimeStamp = 0, finishTime: TimeStamp = 0, user: String = null)
  case class AppMasterDataRequest(appId: Int, detail: Boolean = false)

  case class AppMastersData(appMasters: List[AppMasterData])
  case object AppMastersDataRequest
  case class AppMasterDataDetailRequest(appId: Int)
  case class AppMasterMetricsRequest(appId: Int) extends StreamingType

  case class ReplayFromTimestampWindowTrailingEdge(appId: Int)

  case class WorkerList(workers: List[WorkerId])
}

object AppMasterToWorker {
  case class LaunchExecutor(appId: Int, executorId: Int, resource: Resource, executorJvmConfig: ExecutorJVMConfig)
  case class ShutdownExecutor(appId : Int, executorId : Int, reason : String)
  case class ChangeExecutorResource(appId: Int, executorId: Int, resource: Resource)
}

object WorkerToAppMaster {
  case class ExecutorLaunchRejected(reason: String = null, ex: Throwable = null)
  case class ShutdownExecutorSucceed(appId: Int, executorId: Int)
  case class ShutdownExecutorFailed(reason: String = null, ex: Throwable = null)
}


