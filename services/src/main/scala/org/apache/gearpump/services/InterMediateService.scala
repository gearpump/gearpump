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

import akka.actor.{ActorSystem, ActorRef}
import org.apache.gearpump.util.{Constants, LogUtil}
import spray.http.{StatusCodes, HttpResponse, HttpRequest}
import spray.routing.HttpService
import spray.http.HttpMethods._
import akka.io.IO
import spray.can._
import spray.http.HttpHeader
import spray.http.HttpHeaders.Host
import akka.pattern.ask

import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Failure, Success}

trait InterMediateService extends HttpService {
  def master:ActorRef
  implicit val system: ActorSystem
  lazy val config = system.settings.config

  def extractHost: HttpHeader => Option[Host] = {
    case h: Host => Some(h)
    case x => None
  }

  def interMediateRoute = {
    implicit val ec: ExecutionContext = actorRefFactory.dispatcher
    implicit val timeout = Constants.FUTURE_TIMEOUT
    headerValue(extractHost) { host =>
      pathPrefix("api" / s"$REST_VERSION") {
        // this is a internal API
        path("internal" / "intermediate" / Segment / RestPath) { (command, path) =>
          post {
            val url = s"http://${host.host}:${host.port}/api/$REST_VERSION/$path"
            val request = command match {
              case "delete" => HttpRequest(DELETE, url)
              case "put" => HttpRequest(PUT, url)
            }
            onComplete((IO(Http) ? request).asInstanceOf[Future[HttpResponse]]) {
              case Success(response: HttpResponse) =>
                complete(response.entity)
              case Failure(ex) =>
                complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
            }
          }
        }
      }
    }
  }
}
