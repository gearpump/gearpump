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

package io.gearpump.services

import akka.actor.{ActorSystem, ActorRef}
import akka.http.scaladsl.server.Directives._
import akka.stream.{Materializer}
import io.gearpump.cluster.AppMasterToMaster.{GetWorkerData, WorkerData}
import io.gearpump.cluster.ClientToMaster.{ReadOption, QueryHistoryMetrics, QueryWorkerConfig}
import io.gearpump.cluster.ClusterConfig
import io.gearpump.cluster.MasterToClient.{HistoryMetrics, WorkerConfig}
import io.gearpump.util.ActorUtil._
import io.gearpump.util.Constants

import scala.util.{Failure, Success}

class WorkerService(val master: ActorRef, override val system: ActorSystem)
  extends BasicService {

  import upickle.default.write
  private val systemConfig = system.settings.config
  private val concise = systemConfig.getBoolean(Constants.GEARPUMP_SERVICE_RENDER_CONFIG_CONCISE)

  override def doRoute(implicit mat: Materializer) = pathPrefix("worker" / IntNumber) { workerId =>
    pathEnd {
      onComplete(askWorker[WorkerData](master, workerId, GetWorkerData(workerId))) {
        case Success(value: WorkerData) =>
          complete(write(value.workerDescription))
        case Failure(ex) => failWith(ex)
      }
    } ~
    path("config") {
      onComplete(askWorker[WorkerConfig](master, workerId, QueryWorkerConfig(workerId))) {
        case Success(value: WorkerConfig) =>
          val config = Option(value.config).map(ClusterConfig.render(_, concise)).getOrElse("{}")
          complete(config)
        case Failure(ex) =>
          failWith(ex)
      }
    } ~
    path("metrics" / RestPath ) { path =>
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