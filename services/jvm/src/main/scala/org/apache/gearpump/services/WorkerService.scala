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

package org.apache.gearpump.services

import scala.util.{Failure, Success}

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.server.Directives._
import akka.stream.Materializer

import org.apache.gearpump.cluster.AppMasterToMaster.{GetWorkerData, WorkerData}
import org.apache.gearpump.cluster.ClientToMaster.{QueryHistoryMetrics, QueryWorkerConfig, ReadOption}
import org.apache.gearpump.cluster.ClusterConfig
import org.apache.gearpump.cluster.MasterToClient.{HistoryMetrics, WorkerConfig}
import org.apache.gearpump.cluster.worker.WorkerId
import org.apache.gearpump.util.ActorUtil._
import org.apache.gearpump.util.Constants
// NOTE: This cannot be removed!!!
import org.apache.gearpump.services.util.UpickleUtil._

/** Service to handle worker related queries */
class WorkerService(val master: ActorRef, override val system: ActorSystem)
  extends BasicService {

  import upickle.default.write
  private val systemConfig = system.settings.config
  private val concise = systemConfig.getBoolean(Constants.GEARPUMP_SERVICE_RENDER_CONFIG_CONCISE)

  protected override def doRoute(implicit mat: Materializer) = pathPrefix("worker" / Segment) {
    workerIdString => {
      pathEnd {
        val workerId = WorkerId.parse(workerIdString)
        onComplete(askWorker[WorkerData](master, workerId, GetWorkerData(workerId))) {
          case Success(value: WorkerData) =>
            complete(write(value.workerDescription))
          case Failure(ex) => failWith(ex)
        }
      }
    }~
    path("config") {
      val workerId = WorkerId.parse(workerIdString)
      onComplete(askWorker[WorkerConfig](master, workerId, QueryWorkerConfig(workerId))) {
        case Success(value: WorkerConfig) =>
          val config = Option(value.config).map(ClusterConfig.render(_, concise)).getOrElse("{}")
          complete(config)
        case Failure(ex) =>
          failWith(ex)
      }
    } ~
    path("metrics" / RemainingPath ) { path =>
      val workerId = WorkerId.parse(workerIdString)
      parameter(ReadOption.Key ? ReadOption.ReadLatest) { readOption =>
        val query = QueryHistoryMetrics(path.head.toString, readOption)
        onComplete(askWorker[HistoryMetrics](master, workerId, query)) {
          case Success(value) =>
            complete(write(value))
          case Failure(ex) =>
            failWith(ex)
        }
      }
    }
  }
}