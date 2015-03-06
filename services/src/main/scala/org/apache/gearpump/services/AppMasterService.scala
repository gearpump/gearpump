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
import org.apache.gearpump.cluster.ClientToMaster.ShutdownApplication
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMasterDataDetailRequest, AppMasterDataRequest}
import org.apache.gearpump.cluster.MasterToClient.ShutdownApplicationResult
import org.apache.gearpump.partitioner.Partitioner

import org.apache.gearpump.streaming.appmaster.AppMaster
import org.apache.gearpump.streaming.{AppDescription, TaskDescription}
import org.apache.gearpump.util.{Constants, Graph, LogUtil}
import org.apache.gearpump.streaming.appmaster.{StreamingAppMasterDataDetail, AppMaster}
import org.apache.gearpump.streaming.{AppDescription, TaskDescription, DAG}
import org.apache.gearpump.util.{Graph, Constants}
import spray.http.StatusCodes
import spray.routing.HttpService
import upickle.{Js, Writer}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait AppMasterService extends HttpService  {
  import upickle._
  def master:ActorRef
  implicit val system: ActorSystem
  private val LOG = LogUtil.getLogger(getClass)

  def appMasterRoute = {
    implicit val ec: ExecutionContext = actorRefFactory.dispatcher
    implicit val timeout = Constants.FUTURE_TIMEOUT
    path("appmaster"/IntNumber) { appId =>
      get {
        parameter("detail" ? "false") { detail =>
          val detailValue = Try(detail.toBoolean).getOrElse(false)
          detailValue match {
            case true =>
              onComplete((master ? AppMasterDataDetailRequest(appId)).asInstanceOf[Future[AppMasterDataDetail]]) {
                case Success(value: AppMasterDataDetail) =>
                  complete(value.toJson)
                case Failure(ex) =>
                  complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
              }
            case false =>
              onComplete((master ? AppMasterDataRequest(appId)).asInstanceOf[Future[AppMasterData]]) {
                case Success(value: AppMasterData) => complete(write(value))
                case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
              }
          }
        }
      } ~
      delete {
        onComplete((master ? ShutdownApplication(appId)).asInstanceOf[Future[ShutdownApplicationResult]]) {
          case Success(value: ShutdownApplicationResult) =>
            val result = if (value.appId.isSuccess) Map("status"->"success", "info" -> null) else Map("status" -> "fail", "info" -> value.appId.failed.get.toString)
            complete(write(result))
          case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
        }
      }
    }
  }
}
