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

import akka.actor.{ActorRef, ActorSystem}
import io.gearpump.cluster.AppMasterToMaster.{GetWorkerData, WorkerData}
import io.gearpump.cluster.ClientToMaster.{QueryHistoryMetrics, QueryWorkerConfig}
import io.gearpump.cluster.MasterToClient.{HistoryMetrics, WorkerConfig}
import io.gearpump.util.ActorUtil._
import io.gearpump.util.LogUtil
import spray.routing
import spray.routing.HttpService
import spray.routing.directives.OnCompleteFutureMagnet._

import scala.concurrent.ExecutionContext
import scala.util.{Try, Failure, Success}

trait WorkerService extends HttpService {

  import upickle.default.{read, write}

  def master: ActorRef

  implicit val system: ActorSystem

  private val LOG = LogUtil.getLogger(getClass)

  def workerRoute: routing.Route = {
    implicit val ec: ExecutionContext = actorRefFactory.dispatcher

    pathPrefix("api" / s"$REST_VERSION" / "worker" / IntNumber) { workerId =>
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
            val config = Option(value.config).map(_.root.render()).getOrElse("{}")
            complete(config)
          case Failure(ex) =>
            failWith(ex)
        }
      } ~
      path("metrics" / RestPath ) { path =>
        parameter("readLatest" ? "false") { readLatestInput =>
          val readLatest = Try(readLatestInput.toBoolean).getOrElse(false)
          val query = QueryHistoryMetrics(path.head.toString, readLatest)
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
}