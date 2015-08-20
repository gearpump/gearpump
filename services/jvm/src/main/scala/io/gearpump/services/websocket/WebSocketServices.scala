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

package io.gearpump.services.websocket

import akka.actor._
import akka.io.IO
import io.gearpump.util.{Constants, LogUtil}
import spray.can.Http
import spray.can.server.UHttp

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class WebSocketServices(master: ActorRef) extends Actor with ActorLogging {
  def receive = {
    case Http.Connected(remoteAddress, localAddress) =>
      sender ! Http.Register(WebSocketWorker(sender, master))
  }
}

object WebSocketServices {
  private val LOG = LogUtil.getLogger(getClass)

  def apply(master:ActorRef)(implicit system:ActorSystem) {
    implicit val executionContext = system.dispatcher
    implicit val timeout = Constants.FUTURE_TIMEOUT
    val services = system.actorOf(Props(classOf[WebSocketServices], master),"ws-services")
    val config = system.settings.config

    val port = config.getInt("gearpump.services.ws")

    // web service are allowed to listen on a different hostname or all hostname
    val host = config.getString("gearpump.services.host")

    IO(UHttp) ! Http.Bind(services, interface = host, port = port)
    Option(Await.result(system.actorSelection("user/IO-HTTP").resolveOne(), Duration.Inf)).map(iohttp => {
      system.stop(iohttp)
    })

    LOG.info(s"WebSocket server is enabled http://$host:$port")
    println(s"WebSocket server is enabled http://$host:$port")
  }
}

