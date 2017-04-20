/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.cluster

import org.apache.gearpump.cluster.worker.{WorkerId, WorkerSummary}

import scala.util.Try
import akka.actor.ActorRef
import com.typesafe.config.Config
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.cluster.appmaster.WorkerInfo
import org.apache.gearpump.cluster.master.MasterSummary
import org.apache.gearpump.cluster.scheduler.{Resource, ResourceAllocation, ResourceRequest}
import org.apache.gearpump.metrics.Metrics.MetricType

object ClientToMaster {
  case object AddMaster
  case class AddWorker(count: Int)
  case class RemoveMaster(masterContainerId: String)
  case class RemoveWorker(workerContainerId: String)

  /** Command result of AddMaster, RemoveMaster, and etc... */
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

  /** Submit an application to master */
  case class SubmitApplication(
      appDescription: AppDescription, appJar: Option[AppJar],
      username: String = System.getProperty("user.name"))

  case class RestartApplication(appId: Int)
  case class ShutdownApplication(appId: Int)

  /** Client send ResolveAppId to Master to resolves AppMaster actor path by providing appId */
  case class ResolveAppId(appId: Int)

  /** Client send ResolveWorkerId to master to get the Actor path of worker. */
  case class ResolveWorkerId(workerId: WorkerId)

  /** Get an active Jar store to upload job jars, like wordcount.jar */
  case object GetJarStoreServer

  /** Service address of JarStore */
  case class JarStoreServerAddress(url: String)

  /** Query AppMaster config by providing appId */
  case class QueryAppMasterConfig(appId: Int)

  /** Query worker config */
  case class QueryWorkerConfig(workerId: WorkerId)

  /** Query master config */
  case object QueryMasterConfig

  /** Options for read the metrics from the cluster */
  object ReadOption {
    type ReadOption = String

    val Key: String = "readOption"

    /** Read the latest record of the metrics, only return 1 record for one metric name (id) */
    val ReadLatest: ReadOption = "readLatest"

    /** Read recent metrics from cluster, typically it contains metrics in 5 minutes */
    val ReadRecent = "readRecent"

    /**
     * Read the history metrics, typically it contains metrics for 48 hours
     *
     * NOTE: Each hour only contain one or two data points.
     */
    val ReadHistory = "readHistory"
  }

  /** Query history metrics from master or app master. */
  case class QueryHistoryMetrics(
      path: String, readOption: ReadOption.ReadOption = ReadOption.ReadLatest,
      aggregatorClazz: String = "", options: Map[String, String] = Map.empty[String, String])

  /**
   * If there are message loss, the clock would pause for a while. This message is used to
   * pin-point which task has stalling clock value, and usually it means something wrong on
   * that machine.
   */
  case object GetStallingTasks

  /**
   * Request app master for a short list of cluster app that administrators should be aware of.
   */
  case class GetLastFailure(appId: Int)

  /**
   * Register a client to wait application's result
   */
  case class RegisterAppResultListener(appId: Int)
}

object MasterToClient {

  /** Result of SubmitApplication */
  // TODO: Merge with SubmitApplicationResultValue and change this to (appId: Option, ex: Exception)
  case class SubmitApplicationResult(appId: Try[Int])

  case class SubmitApplicationResultValue(appId: Int)

  case class ShutdownApplicationResult(appId: Try[Int])
  case class ReplayApplicationResult(appId: Try[Int])

  /** Return Actor ref of app master */
  case class ResolveAppIdResult(appMaster: Try[ActorRef])

  /** Return Actor ref of worker */
  case class ResolveWorkerIdResult(worker: Try[ActorRef])

  case class AppMasterConfig(config: Config)

  case class WorkerConfig(config: Config)

  case class MasterConfig(config: Config)

  case class HistoryMetricsItem(time: TimeStamp, value: MetricType)

  /**
   * History metrics returned from master, worker, or app master.
   *
   * All metric items are organized like a tree, path is used to navigate through the tree.
   * For example, when querying with path == "executor0.task1.throughput*", the metrics
   * provider picks metrics whose source matches the path.
   *
   * @param path The path client provided. The returned metrics are the result query of this path.
   * @param metrics The detailed metrics.
   */
  case class HistoryMetrics(path: String, metrics: List[HistoryMetricsItem])

  /** Return the last error of this streaming application job */
  case class LastFailure(time: TimeStamp, error: String)

  sealed trait ApplicationResult

  case class ApplicationSucceeded(appId: Int) extends ApplicationResult

  case class ApplicationFailed(appId: Int, error: Throwable) extends ApplicationResult
}

object AppMasterToMaster {
  
  /**
   * Register an AppMaster by providing a ActorRef, and workerInfo which is running on
   */
  case class RegisterAppMaster(appId: Int, appMaster: ActorRef, workerInfo: WorkerInfo)

  case class InvalidAppMaster(appId: Int, appMaster: String, reason: Throwable)

  case class RequestResource(appId: Int, request: ResourceRequest)

  /**
   * Each application job can save some data in the distributed cluster storage on master nodes.
   *
   * @param appId App Id of the client application who send the request.
   * @param key Key name
   * @param value Value to store on distributed cluster storage on master nodes
   */
  case class SaveAppData(appId: Int, key: String, value: Any)

  /** The application specific data is successfully stored */
  case object AppDataSaved

  /** Fail to store the application data */
  case object SaveAppDataFailed

  /** Fetch the application specific data that stored previously */
  case class GetAppData(appId: Int, key: String)

  /** The KV data returned for query GetAppData */
  case class GetAppDataResult(key: String, value: Any)

  /**
   * AppMasterSummary returned to REST API query. Streaming and Non-streaming
   * have very different application info. AppMasterSummary is the common interface.
   */
  trait AppMasterSummary {
    def appType: String
    def appId: Int
    def appName: String
    def actorPath: String
    def status: ApplicationStatus
    def startTime: TimeStamp
    def uptime: TimeStamp
    def user: String
  }

  /** Represents a generic application that is not a streaming job */
  case class GeneralAppMasterSummary(
      appId: Int,
      appType: String = "general",
      appName: String = null,
      actorPath: String = null,
      status: ApplicationStatus = ApplicationStatus.ACTIVE,
      startTime: TimeStamp = 0L,
      uptime: TimeStamp = 0L,
      user: String = null)
    extends AppMasterSummary

  /** Fetches the list of workers from Master */
  case object GetAllWorkers

  /** Get worker data of workerId */
  case class GetWorkerData(workerId: WorkerId)

  /** Response to GetWorkerData */
  case class WorkerData(workerDescription: WorkerSummary)

  /** Get Master data */
  case object GetMasterData

  /** Response to GetMasterData */
  case class MasterData(masterDescription: MasterSummary)

  /**
   * Denotes the application state change of an app.
   */
  case class ApplicationStatusChanged(appId: Int, newStatus: ApplicationStatus,
      timeStamp: TimeStamp, error: Throwable)
}

object MasterToAppMaster {

  /** Resource allocated for application xx */
  case class ResourceAllocated(allocations: Array[ResourceAllocation])

  /** Master confirm reception of RegisterAppMaster message */
  case class AppMasterRegistered(appId: Int)

  /** Master confirm reception of ActivateAppMaster message */
  case class AppMasterActivated(appId: Int)

  /** Shutdown the application job */
  case object ShutdownAppMaster

  sealed trait StreamingType
  case class AppMasterData(status: ApplicationStatus, appId: Int = 0, appName: String = null,
      appMasterPath: String = null, workerPath: String = null, submissionTime: TimeStamp = 0,
      startTime: TimeStamp = 0, finishTime: TimeStamp = 0, user: String = null)

  case class AppMasterDataRequest(appId: Int, detail: Boolean = false)

  case class AppMastersData(appMasters: List[AppMasterData])
  case object AppMastersDataRequest
  case class AppMasterDataDetailRequest(appId: Int)
  case class AppMasterMetricsRequest(appId: Int) extends StreamingType

  case class ReplayFromTimestampWindowTrailingEdge(appId: Int)

  case class WorkerList(workers: List[WorkerId])
}

object AppMasterToWorker {
  case class LaunchExecutor(
      appId: Int, executorId: Int, resource: Resource, executorJvmConfig: ExecutorJVMConfig)

  case class ShutdownExecutor(appId: Int, executorId: Int, reason: String)
  case class ChangeExecutorResource(appId: Int, executorId: Int, resource: Resource)
}

object WorkerToAppMaster {
  case class ExecutorLaunchRejected(reason: String = null, ex: Throwable = null)
  case class ShutdownExecutorSucceed(appId: Int, executorId: Int)
  case class ShutdownExecutorFailed(reason: String = null, ex: Throwable = null)
}

