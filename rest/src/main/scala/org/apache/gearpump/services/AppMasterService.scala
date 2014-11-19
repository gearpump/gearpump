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

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import com.wordnik.swagger.annotations._
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMasterDataDetail, AppMasterDataDetailRequest, AppMasterDataRequest}
import org.apache.gearpump.util.Constants
import spray.http.StatusCodes
import spray.routing.HttpService

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

@Api(value = "/appmaster", description = "AppMaster Info.")
class AppMasterServiceActor(val master:ActorRef) extends Actor with AppMasterService   {
  def actorRefFactory = context
  def receive = runRoute(readRoute)
}

@Api(value = "/appmaster", description = "AppMaster Info.")
trait AppMasterService extends HttpService {
  import org.apache.gearpump.services.Json4sSupport._
  implicit val ec: ExecutionContext = actorRefFactory.dispatcher
  implicit val timeout = Constants.FUTURE_TIMEOUT
  val master:ActorRef

  val routes = readRoute 

  @ApiOperation(value = "Get AppMaster Info", notes = "Returns AppMaster Info ", httpMethod = "GET", response = classOf[AppMasterData])
  @ApiImplicitParams(Array(
      new ApiImplicitParam(name = "appId", required = true, dataType = "integer", paramType = "path", value = "ID of AppMaster"),
      new ApiImplicitParam(name = "detail", required = false, dataType = "boolean", paramType = "query", value = "true/false")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 404, message = "AppMaster not found"),
    new ApiResponse(code = 400, message = "Invalid ID supplied")
  ))
  def readRoute = get { 
     path("appmaster"/IntNumber) { appId => {
       parameter("detail" ? "false") { detail =>
         val detailValue = Try(detail.toBoolean).getOrElse(false)
         detailValue match {
           case true =>
             onComplete((master ? AppMasterDataDetailRequest(appId)).asInstanceOf[Future[AppMasterDataDetail]]) {
               case Success(value: AppMasterDataDetail) => complete(value)
               case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
             }
           case false =>
             onComplete((master ? AppMasterDataRequest(appId)).asInstanceOf[Future[AppMasterData]]) {
               case Success(value: AppMasterData) => complete(value)
               case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
             }
         }
       }
     }
    }
  }
}
