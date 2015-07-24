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
package org.apache.gearpump.services.websocket

import akka.actor.ActorSystem
import org.apache.gearpump.services.REST_VERSION
import org.apache.gearpump.services.websocket.WebSocketService.WebSocketUrl
import org.apache.gearpump.util.Constants
import spray.routing.HttpService

import scala.concurrent.ExecutionContext

trait WebSocketService extends HttpService{
  import upickle.default.{read, write}
  implicit val system: ActorSystem

  def webSocketRoute = {
    implicit val ec: ExecutionContext = actorRefFactory.dispatcher
    implicit val timeout = Constants.FUTURE_TIMEOUT
    val systemConfig = system.settings.config
    hostName { hostname =>
      pathPrefix("api" / s"$REST_VERSION") {
        path("websocket" / "url") {
          val port = systemConfig.getInt("gearpump.services.ws")
          val host = hostname
          complete(write(WebSocketUrl(s"ws://$host:$port")))
        }
      }
    }
  }
}

object WebSocketService {
  case class WebSocketUrl(url: String)
}
