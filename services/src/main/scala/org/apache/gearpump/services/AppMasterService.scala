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
import org.apache.gearpump._
import org.apache.gearpump.cluster.AppMasterToMaster.AppMasterDataDetail
import org.apache.gearpump.cluster.ClientToMaster.{QueryHistoryMetrics, ResolveAppId, GetStallingTasks, ShutdownApplication}
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMasterDataDetailRequest, AppMasterDataRequest}
import org.apache.gearpump.cluster.MasterToClient.{HistoryMetrics, ResolveAppIdResult, ShutdownApplicationResult}
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.streaming.AppMasterToMaster.StallingTasks

import org.apache.gearpump.streaming.appmaster.AppMaster
import org.apache.gearpump.streaming.appmaster.DagManager.{ReplaceProcessor, DAGOperationResult, DAGOperation}
import org.apache.gearpump.streaming.{ProcessorDescription}
import org.apache.gearpump.util.{Constants, Graph, LogUtil}
import org.apache.gearpump.streaming.appmaster.{StreamingAppMasterDataDetail, AppMaster}
import org.apache.gearpump.streaming.{ProcessorDescription, DAG}
import org.apache.gearpump.util.{Graph, Constants}
import spray.http.StatusCodes
import spray.routing.HttpService
import spray.routing.Route
import spray.routing.directives.OnCompleteFutureMagnet
import upickle.{Js, Writer}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

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
            finish(toAppMaster[DAGOperationResult](appId, dagOperation))
          }
        }
      } ~
      path("stallingtasks") {
        finish(toAppMaster[StallingTasks](appId, GetStallingTasks(appId)))
      } ~
      path("metrics" / RestPath ) { path =>
        parameter("readLatest" ? "false") { readLatestInput =>
          val readLatest = Try(readLatestInput.toBoolean).getOrElse(false)
          val query = QueryHistoryMetrics(appId, path.head.toString, readLatest)
          finish(toAppMaster[HistoryMetrics](appId, query))
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
                val future = toAppMaster[AppMasterDataDetail](appId, request)
                finish(future, writer)
              case false =>
                finish(sendToMaster[AppMasterData](AppMasterDataRequest(appId)))
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
            finish(sendToMaster[ShutdownApplicationResult](ShutdownApplication(appId)), writer)
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

  private def toAppMaster[T](appId: Int, msg: Any)(implicit timeout: akka.util.Timeout, ex: ExecutionContext): Future[T] = {
    val appmaster = (master ? ResolveAppId(appId)).asInstanceOf[Future[ResolveAppIdResult]].flatMap { result =>
      if (result.appMaster.isSuccess) {
        Future.successful(result.appMaster.get)
      } else {
        Future.failed(result.appMaster.failed.get)
      }
    }
    appmaster.flatMap { appMaster =>
      (appMaster ? msg).asInstanceOf[Future[T]]
    }
  }

  private def sendToMaster[T](msg: Any)(implicit timeout: akka.util.Timeout, ex: ExecutionContext): Future[T] = {
    (master ? msg).asInstanceOf[Future[T]]
  }
}

object GOOD extends App {
  val oldProcessorId = 1
  val conf = UserConfig.empty.withString("key", "value")
  val newProcessorDescription = ProcessorDescription(1, "org.apache.gearpump.streaming.examples.wordcount.Sum", 2, "description", conf)
  val replace = ReplaceProcessor(oldProcessorId, newProcessorDescription)
  Console.println(upickle.write(replace))
}