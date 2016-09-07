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

import scala.util.{Failure, Success, Try}

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.{FormData, Multipart}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.ParameterDirectives.ParamMagnet
import akka.stream.Materializer
import upickle.default.{read, write}

import org.apache.gearpump.cluster.AppMasterToMaster.{AppMasterSummary, GeneralAppMasterSummary}
import org.apache.gearpump.cluster.ClientToMaster._
import org.apache.gearpump.cluster.ClusterConfig
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMasterDataDetailRequest, AppMasterDataRequest}
import org.apache.gearpump.cluster.MasterToClient._
import org.apache.gearpump.jarstore.{JarStoreClient, FileDirective}
import org.apache.gearpump.services.AppMasterService.Status
// NOTE: This cannot be removed!!!
import org.apache.gearpump.services.util.UpickleUtil._
import org.apache.gearpump.streaming.AppMasterToMaster.StallingTasks
import org.apache.gearpump.streaming.appmaster.DagManager._
import org.apache.gearpump.streaming.appmaster.StreamAppMasterSummary
import org.apache.gearpump.streaming.executor.Executor.{ExecutorConfig, ExecutorSummary, GetExecutorSummary, QueryExecutorConfig}
import org.apache.gearpump.util.ActorUtil.{askActor, askAppMaster}
import FileDirective._
import org.apache.gearpump.util.{Constants, Util}

/**
 * Management service for AppMaster
 */
class AppMasterService(val master: ActorRef,
    val jarStoreClient: JarStoreClient, override val system: ActorSystem)
  extends BasicService {

  private val systemConfig = system.settings.config
  private val concise = systemConfig.getBoolean(Constants.GEARPUMP_SERVICE_RENDER_CONFIG_CONCISE)

  protected override def doRoute(implicit mat: Materializer) = pathPrefix("appmaster" / IntNumber) {
    appId => {
      path("dynamicdag") {
        parameters(ParamMagnet("args")) { args: String =>
          def replaceProcessor(dagOperation: DAGOperation): Route = {
            onComplete(askAppMaster[DAGOperationResult](master, appId, dagOperation)) {
              case Success(value) =>
                complete(write(value))
              case Failure(ex) =>
                failWith(ex)
            }
          }

          val msg = java.net.URLDecoder.decode(args, "UTF-8")
          val dagOperation = read[DAGOperation](msg)
          (post & entity(as[Multipart.FormData])) { _ =>
            uploadFile { form =>
              val jar = form.getFileInfo("jar").map(_.file)

              if (jar.nonEmpty) {
                dagOperation match {
                  case replace: ReplaceProcessor =>
                    val description = replace.newProcessorDescription.copy(jar =
                      Util.uploadJar(jar.get, jarStoreClient))
                    val dagOperationWithJar = replace.copy(newProcessorDescription = description)
                    replaceProcessor(dagOperationWithJar)
                }
              } else {
                replaceProcessor(dagOperation)
              }
            }
          } ~ (post & entity(as[FormData])) { _ =>
            replaceProcessor(dagOperation)
          }
        }
      } ~
      path("stallingtasks") {
        onComplete(askAppMaster[StallingTasks](master, appId, GetStallingTasks(appId))) {
          case Success(value) =>
            complete(write(value))
          case Failure(ex) => failWith(ex)
        }
      } ~
      path("errors") {
        onComplete(askAppMaster[LastFailure](master, appId, GetLastFailure(appId))) {
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
      } ~
      path("config") {
        onComplete(askActor[AppMasterConfig](master, QueryAppMasterConfig(appId))) {
          case Success(value: AppMasterConfig) =>
            val config = Option(value.config).map(ClusterConfig.render(_, concise)).getOrElse("{}")
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
              val config = Option(value.config).map(ClusterConfig.render(_, concise))
                .getOrElse("{}")
              complete(config)
            case Failure(ex) =>
              failWith(ex)
          }
        } ~
          pathEnd {
            get {
              val executorId = Integer.parseInt(executorIdString)
              onComplete(askAppMaster[ExecutorSummary](master, appId,
            GetExecutorSummary(executorId))) {
              case Success(value) =>
                complete(write(value))
              case Failure(ex) =>
                failWith(ex)
              }
            }
          }
      } ~
      path("metrics" / RestPath) { path =>
        parameterMap { optionMap =>
          parameter("aggregator" ? "") { aggregator =>
            parameter(ReadOption.Key ? ReadOption.ReadLatest) { readOption =>
              val query = QueryHistoryMetrics(path.head.toString, readOption, aggregator, optionMap)
              onComplete(askAppMaster[HistoryMetrics](master, appId, query)) {
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
        get {
          parameter("detail" ? "false") { detail =>
            val queryDetails = Try(detail.toBoolean).getOrElse(false)
            val request = AppMasterDataDetailRequest(appId)
            queryDetails match {
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