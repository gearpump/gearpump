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
import org.apache.gearpump.cluster.AppMasterToMaster.{GeneralAppMasterDataDetail, AppMasterDataDetail}
import org.apache.gearpump.cluster.ClientToMaster.{GetStallingTasks, QueryAppMasterConfig, QueryHistoryMetrics, RestartApplication, ShutdownApplication}
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMasterDataDetailRequest, AppMasterDataRequest}
import org.apache.gearpump.cluster.MasterToClient.{AppMasterConfig, HistoryMetrics, ShutdownApplicationResult, SubmitApplicationResult}
import org.apache.gearpump.services.AppMasterService.Status
import org.apache.gearpump.services.util.UpickleUtil._
import org.apache.gearpump.streaming.AppMasterToMaster.StallingTasks
import org.apache.gearpump.streaming.appmaster.DagManager.{DAGOperation, DAGOperationResult}
import org.apache.gearpump.streaming.appmaster.StreamingAppMasterDataDetail
import org.apache.gearpump.util.ActorUtil.{askActor, askAppMaster}
import org.apache.gearpump.util.{Constants, LogUtil}
import spray.routing.HttpService
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}
import upickle.default.{write, read}

trait AppMasterService extends HttpService {

  def master: ActorRef

  implicit val system: ActorSystem

  def appMasterRoute = {
    implicit val timeout = Constants.FUTURE_TIMEOUT
    implicit val ec: ExecutionContext = actorRefFactory.dispatcher
    pathPrefix("api" / s"$REST_VERSION" / "appmaster" / IntNumber ) { appId =>
      path("dynamicdag") {
        post {
          entity(as[String]) { entity =>
            val dagOperation = read[DAGOperation](entity)
            onComplete(askAppMaster[DAGOperationResult](master, appId, dagOperation)) {
              case Success(value) =>
                complete(write(value))
              case Failure(ex) => failWith(ex)
            }
          }
        }
      } ~
      path("stallingtasks") {
        onComplete(askAppMaster[StallingTasks](master, appId, GetStallingTasks(appId))){
          case Success(value) =>
            complete(write(value))
          case Failure(ex) => failWith(ex)
        }
      }  ~
      path("restart") {
        post {
          onComplete(askActor[SubmitApplicationResult](master, RestartApplication(appId))) {
            case Success(_) =>
              complete(write(Status(true)))
            case Failure(ex) =>
              complete(write(Status(false, ex.getMessage)))
          }
        }
      }~
      path("config") {
        onComplete(askActor[AppMasterConfig](master, QueryAppMasterConfig(appId))) {
          case Success(value: AppMasterConfig) =>
            val config = Option(value.config).map(_.root.render()).getOrElse("{}")
            complete(config)
          case Failure(ex) =>
            failWith(ex)
        }
      } ~
      path("metrics" / RestPath ) { path =>
        parameter("readLatest" ? "false") { readLatestInput =>
          val readLatest = Try(readLatestInput.toBoolean).getOrElse(false)
          val query = QueryHistoryMetrics(appId, path.head.toString, readLatest)
          onComplete(askAppMaster[HistoryMetrics](master, appId, query)) {
            case Success(value) =>
              complete(write(value))
            case Failure(ex) =>
              failWith(ex)
          }
        }
      } ~
      pathEnd {
        get {
          parameter("detail" ? "false") { detail =>
            val detailValue = Try(detail.toBoolean).getOrElse(false)
            val request = AppMasterDataDetailRequest(appId)
            detailValue match {
              case true =>
                onComplete(askAppMaster[AppMasterDataDetail](master, appId, request)) {
                  case Success(value) =>
                    value match {
                      case data: GeneralAppMasterDataDetail =>
                        complete(write(data))
                      case data: StreamingAppMasterDataDetail =>
                        complete(write(data))
                    }
                  case Failure(ex) =>
                    failWith(ex)
                }

              case false =>
                onComplete(askActor[AppMasterData](master, AppMasterDataRequest(appId))) {
                  case Success(value) =>
                    complete(write(value))
                  case Failure(ex) =>
                    failWith(ex)
                }
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
              write(output)
            }
            onComplete(askActor[ShutdownApplicationResult](master, ShutdownApplication(appId))) {
              case Success(result) =>
                val output = if (result.appId.isSuccess) {
                  Map("status" -> "success", "info" -> null)
                } else {
                  Map("status" -> "fail", "info" -> result.appId.failed.get.toString)
                }
                complete(write(output))
              case Failure(ex) =>
                failWith(ex)
            }
          }
      }
    }
  }
}

object AppMasterService {
  case class Status(success: Boolean, reason: String = null)
}