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
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMastersData, AppMastersDataRequest}
import org.apache.gearpump.util.Constants
import spray.http.StatusCodes
import spray.routing.HttpService

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

@Api(value = "/appmasters", description = "AppMasters Info.")
class AppMastersServiceActor(val master:ActorRef) extends Actor with AppMastersService   {
  def actorRefFactory = context
  def receive = runRoute(readRoute)

}
@Api(value = "/appmasters", description = "AppMasters Info.")
trait AppMastersService extends HttpService {
  import org.apache.gearpump.services.AppMasterProtocol._
  import spray.httpx.SprayJsonSupport._
  implicit val ec: ExecutionContext = actorRefFactory.dispatcher
  implicit val timeout = Constants.FUTURE_TIMEOUT
  val master:ActorRef

  val routes = readRoute

  @ApiOperation(value = "Get AppMasters Info", notes = "Returns AppMasters Info ", httpMethod = "GET", response = classOf[AppMastersData])
  @ApiResponses(Array(
    new ApiResponse(code = 404, message = "AppMasters not found")
  ))
  def readRoute = get {
    path("appmasters") {
      onComplete((master ? AppMastersDataRequest).asInstanceOf[Future[AppMastersData]]) {
        case Success(value:AppMastersData) =>
          complete(value)
        case Failure(ex)    => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
      }
    }
  }
}

