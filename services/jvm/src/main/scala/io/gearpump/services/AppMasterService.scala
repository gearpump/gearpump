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

import java.io.File

import akka.actor.{ActorRef}
import akka.http.scaladsl.server.directives.ParameterDirectives.ParamMagnet
import io.gearpump.cluster.AppMasterToMaster.{AppMasterSummary, GeneralAppMasterSummary}
import io.gearpump.cluster.ClientToMaster._
import io.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMasterDataDetailRequest, AppMasterDataRequest}
import io.gearpump.cluster.MasterToClient._
import io.gearpump.services.AppMasterService.Status
import io.gearpump.streaming.AppMasterToMaster.StallingTasks
import io.gearpump.streaming.appmaster.DagManager._
import io.gearpump.streaming.appmaster.{DagManager, StreamAppMasterSummary}
import io.gearpump.streaming.executor.Executor.{ExecutorConfig, ExecutorSummary, GetExecutorSummary, QueryExecutorConfig}
import io.gearpump.util.ActorUtil.{askActor, askAppMaster}
import io.gearpump.util.{FileDirective, Constants, Util}
import upickle.default.{read, write}

import scala.util.{Failure, Success, Try}
import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives._
import scala.concurrent.{ExecutionContext}
import FileDirective._

trait AppMasterService  {
  this: JarStoreProvider =>

  def master: ActorRef
  implicit def system: ActorSystem

  def appMasterRoute = {
    implicit val timeout = Constants.FUTURE_TIMEOUT
    implicit def ec: ExecutionContext = system.dispatcher

    pathPrefix("api" / s"$REST_VERSION" / "appmaster" / IntNumber ) { appId =>
      path("dynamicdag") {
        post {
          parameters(ParamMagnet("args")) { args: String =>
            uploadFile { fileMap =>
              val jar = fileMap.get("jar").map(_.file)
              val msg = java.net.URLDecoder.decode(args)
              var dagOperation = read[DAGOperation](msg)
              if(jar.nonEmpty) {
                dagOperation match {
                  case replace: ReplaceProcessor =>
                    val description = replace.newProcessorDescription.copy(jar = Util.uploadJar(jar.get, getJarStoreService))
                    dagOperation = replace.copy(newProcessorDescription = description)
                }
              }
              onComplete(askAppMaster[DAGOperationResult](master, appId, dagOperation)) {
                case Success(value) =>
                  complete(write(value))
                case Failure(ex) => failWith(ex)
              }
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
      path("errors") {
        onComplete(askAppMaster[LastFailure](master, appId, GetLastFailure(appId))){
          case Success(value) =>
            complete(write(value))
          case Failure(ex) => failWith(ex)
        }
      } ~
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
      pathPrefix("executor" / Segment) { executorIdString =>
        path("config") {
          val executorId = Integer.parseInt(executorIdString)
          onComplete(askAppMaster[ExecutorConfig](master, appId, QueryExecutorConfig(executorId))) {
            case Success(value) =>
              val config = Option(value.config).map(_.root.render()).getOrElse("{}")
              complete(config)
            case Failure(ex) =>
              failWith(ex)
          }
        } ~
        pathEnd {
          get {
            val executorId = Integer.parseInt(executorIdString)
            onComplete(askAppMaster[ExecutorSummary](master, appId, GetExecutorSummary(executorId))) {
              case Success(value) =>
                complete(write(value))
              case Failure(ex) =>
                failWith(ex)
            }
          }
        }
      } ~
      path("metrics" / RestPath ) { path =>
        parameter("readLatest" ? "false") { readLatestInput =>
          val readLatest = Try(readLatestInput.toBoolean).getOrElse(false)
          val query = QueryHistoryMetrics(path.head.toString, readLatest)
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
                onComplete(askAppMaster[AppMasterSummary](master, appId, request)) {
                  case Success(value) =>
                    value match {
                      case data: GeneralAppMasterSummary =>
                        complete(write(data))
                      case data: StreamAppMasterSummary =>
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