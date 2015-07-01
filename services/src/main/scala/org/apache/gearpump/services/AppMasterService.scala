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
import org.apache.gearpump.cluster.AppMasterToMaster.AppMasterDataDetail

import org.apache.gearpump.cluster.ClientToMaster.{RestartApplication}
import org.apache.gearpump.cluster.MasterToClient.{SubmitApplicationResult}
import org.apache.gearpump.services.AppMasterService.Status
import org.apache.gearpump.cluster.ClientToMaster.{QueryHistoryMetrics, GetStallingTasks, ShutdownApplication}
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMasterDataDetailRequest, AppMasterDataRequest}
import org.apache.gearpump.cluster.MasterToClient.{HistoryMetrics, ShutdownApplicationResult}
import org.apache.gearpump.streaming.AppMasterToMaster.StallingTasks

import org.apache.gearpump.streaming.appmaster.DagManager.{DAGOperationResult, DAGOperation}

import org.apache.gearpump.util.{LogUtil}
import org.apache.gearpump.util.{Constants}
import spray.http.StatusCodes
import spray.routing.HttpService
import spray.routing.Route
import spray.routing.directives.OnCompleteFutureMagnet



import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

import org.apache.gearpump.util.ActorUtil.{askAppMaster, askActor}
trait AppMasterService extends HttpService {

  import upickle._

  def master: ActorRef

  implicit val system: ActorSystem
  private val LOG = LogUtil.getLogger(getClass)

  def appMasterRoute = {

    implicit val timeout = Constants.FUTURE_TIMEOUT
    implicit val ec: ExecutionContext = actorRefFactory.dispatcher

    pathPrefix("api" / s"$REST_VERSION" / "appmaster" / IntNumber ) { appId =>
      path("dynamicdag") {
        (post) {
          entity(as[String]) { entity =>
            val dagOperation = read[DAGOperation](entity)
            finish(askAppMaster[DAGOperationResult](master, appId, dagOperation))
          }
        }
      } ~
      path("stallingtasks") {
        finish(askAppMaster[StallingTasks](master, appId, GetStallingTasks(appId)))
      }  ~
      path("appmaster" / IntNumber / "restart") { appId =>
        post {
          onComplete(askActor[SubmitApplicationResult](master, RestartApplication(appId))) {
            case Success(_) =>
              complete(write(Status(true)))
            case Failure(ex) =>
              complete(write(Status(false, ex.getMessage)))
          }
        }
      }~
      path("metrics" / RestPath ) { path =>
        parameter("readLatest" ? "false") { readLatestInput =>
          val readLatest = Try(readLatestInput.toBoolean).getOrElse(false)
          val query = QueryHistoryMetrics(appId, path.head.toString, readLatest)
          finish(askAppMaster[HistoryMetrics](master, appId, query))
        }
      } ~
      pathEnd {
        get {
          parameter("detail" ? "false") { detail =>
            val detailValue = Try(detail.toBoolean).getOrElse(false)
            val request = AppMasterDataDetailRequest(appId)
            detailValue match {
              case true =>
                val writer: AppMasterDataDetail => String = (detail) => detail.toJson
                val future = askAppMaster[AppMasterDataDetail](master, appId, request)
                finish(future, writer)
              case false =>
                finish(askActor[AppMasterData](master, AppMasterDataRequest(appId)))
            }
          }
        }
      } ~
      pathEnd {
          delete {
            val writer = (result: ShutdownApplicationResult) => {
              val output = if (result.appId.isSuccess) {
                Map("status" -> "success", "info" -> null)
              } else {
                Map("status" -> "fail", "info" -> result.appId.failed.get.toString)
              }
              upickle.write(output)
            }
            finish(askActor[ShutdownApplicationResult](master, ShutdownApplication(appId)), writer)
          }
      }
    }
  }

  private def finish[T](future: OnCompleteFutureMagnet[T])(implicit evidence: upickle.Writer[T]): Route = {
    onComplete(future) {
      case Success(value) =>
          complete(upickle.write(value))
      case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
    }
  }

  private def finish[T](future: OnCompleteFutureMagnet[T], writer: T => String): Route = {
    onComplete(future) {
      case Success(value) =>
        complete(writer(value))
      case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
    }
  }
}

object AppMasterService {
  case class Status(success: Boolean, reason: String = null)
}