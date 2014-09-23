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


import java.util.concurrent.TimeUnit

import akka.actor.{ActorContext, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import com.wordnik.swagger.annotations._
import org.apache.gearpump.cluster.{AppMasterInfo, AppMasterRegisterData}
import spray.http.StatusCodes
import spray.routing.HttpService

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}


@Api(value = "/appmaster", description = "AppMaster Info.")
class AppMasterService(val master:ActorRef, val context: ActorContext, executionContext: ExecutionContext) extends HttpService {
  import org.apache.gearpump.services.Json4sSupport._
  implicit val timeout = Timeout(5, TimeUnit.SECONDS)
  def actorRefFactory = context
  implicit val executionContextRef:ExecutionContext = executionContext

  val routes = readRoute 

  @ApiOperation(value = "Get AppMaster Info", notes = "Returns AppMaster Info ", httpMethod = "GET", response = classOf[AppMasterData])
  @ApiResponses(Array(
    new ApiResponse(code = 404, message = "AppMaster not found"),
    new ApiResponse(code = 400, message = "Invalid ID supplied")
  ))
  def readRoute = get { 
     path("appmaster") {
       onComplete((master ? AppMasterDataRequest()).asInstanceOf[Future[AppMasterData]]) {
         case Success(value:AppMasterData) => complete(value)
         case Failure(ex)    => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
       }
    }
  }
}

case class AppMasterData(appId: Int, executorId: Int, appData: AppMasterInfo)
case class AppMasterDataRequest()


