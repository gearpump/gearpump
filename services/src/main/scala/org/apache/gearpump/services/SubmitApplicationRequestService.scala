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
import org.apache.gearpump.cluster.AppMasterToMaster.AppMasterDataDetail
import org.apache.gearpump.cluster.MasterToAppMaster.AppMasterDataDetailRequest
import org.apache.gearpump.cluster.client.ClientSubmitter
import org.apache.gearpump.streaming.StreamApplication
import org.apache.gearpump.streaming.appmaster.SubmitApplicationRequest
import org.apache.gearpump.util.Constants
import spray.http.StatusCodes
import spray.routing.HttpService

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait SubmitApplicationRequestService extends HttpService with ClientSubmitter  {
  implicit val timeout = Constants.FUTURE_TIMEOUT
  implicit val system: ActorSystem
  def master:ActorRef

  def applicationRequestRoute = {
    implicit val ec: ExecutionContext = actorRefFactory.dispatcher
    pathPrefix("api"/s"$REST_VERSION") {
      path("application") {
        post {
          import SubmitApplicationRequest._
          entity(as[SubmitApplicationRequest]) { submitApplicationRequest =>
            import submitApplicationRequest.{appJar, appName, processors, dag}
            val appId = submit(StreamApplication(appName, appJar, processors, dag))
            onComplete((master ? AppMasterDataDetailRequest(appId)).asInstanceOf[Future[AppMasterDataDetail]]) {
              case Success(value: AppMasterDataDetail) =>
                complete(value.toJson)
              case Failure(ex) =>
                complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
            }

          }
        }
      }
    }
  }
}
