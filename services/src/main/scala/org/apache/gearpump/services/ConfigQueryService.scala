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

package org.apache.gearpump.services

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.AppMasterToMaster.{WorkerData, GetWorkerData, AppMasterDataDetail}
import org.apache.gearpump.cluster.ClientToMaster.{QueryMasterConfig, QueryWorkerConfig, QueryAppMasterConfig, ShutdownApplication}
import org.apache.gearpump.cluster.MasterToAppMaster.{WorkerList, AppMasterData, AppMasterDataDetailRequest, AppMasterDataRequest}
import org.apache.gearpump.cluster.MasterToClient.{MasterConfig, WorkerConfig, AppMasterConfig, ShutdownApplicationResult}
import org.apache.gearpump.cluster.worker.WorkerDescription
import org.apache.gearpump.util.{Constants, LogUtil}
import spray.http.StatusCodes
import spray.routing.HttpService

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait ConfigQueryService extends HttpService  {
  import upickle._
  def master:ActorRef
  implicit val system: ActorSystem
  private val LOG = LogUtil.getLogger(getClass)

  def configQueryRoute = get {
    implicit val ec: ExecutionContext = actorRefFactory.dispatcher
    implicit val timeout = Constants.FUTURE_TIMEOUT
    pathPrefix("api"/s"$REST_VERSION") {
      path("config" / "app" / IntNumber) { appId =>
        onComplete((master ? QueryAppMasterConfig(appId)).asInstanceOf[Future[AppMasterConfig]]) {
          case Success(value: AppMasterConfig) =>
            val config = Option(value.config).map(_.root.render()).getOrElse("{}")
            complete(config)
          case Failure(ex) =>
            complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
        }
      } ~
      path("config" / "worker" / IntNumber) { workerId =>

        def workerDataFuture(workerId: Int): Future[WorkerConfig] = {
          (master ? GetWorkerData(workerId)).asInstanceOf[Future[WorkerData]].flatMap { workerData =>

            workerData.workerDescription.map { workerDescription =>
              system.actorSelection(workerDescription.actorPath)
            }.map { workerActor =>
              (workerActor ? QueryWorkerConfig(workerId)).asInstanceOf[Future[WorkerConfig]]
            }.getOrElse(Future(WorkerConfig(ConfigFactory.empty)))
          }
        }
        onComplete(workerDataFuture(workerId)) {
          case Success(value: WorkerConfig) =>
            val config = Option(value.config).map(_.root.render()).getOrElse("{}")
            complete(config)
          case Failure(ex) =>
            complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
        }
      } ~
      path("config" / "master") {
        onComplete((master ? QueryMasterConfig).asInstanceOf[Future[MasterConfig]]) {
          case Success(value: MasterConfig) =>
            val config = Option(value.config).map(_.root.render()).getOrElse("{}")
            complete(config)
          case Failure(ex) =>
            complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
        }
      }
    }
  }
}