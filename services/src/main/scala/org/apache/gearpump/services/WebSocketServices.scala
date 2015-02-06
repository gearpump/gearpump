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

import akka.actor._
import akka.io.IO
import org.apache.gearpump.cluster.ClusterConfig
import spray.can.Http
import spray.can.server.UHttp

class WebSocketServices(masters: ActorRef) extends Actor with ActorLogging {
  def receive = {
    // when a new connection comes in we register a WebSocketConnection actor as the per connection handler
    case Http.Connected(remoteAddress, localAddress) =>
      val serverConnection = sender()
      val conn = context.actorOf(WebSocketWorker.props(serverConnection))
      serverConnection ! Http.Register(conn)
  }
}

object WebSocketServices {
  def start(master:ActorRef) {
    implicit val system = ActorSystem("ws-services" , ClusterConfig.load.application)
    implicit val executionContext = system.dispatcher
    val services = system.actorOf(Props(classOf[WebSocketServices], master),"ws-services")
    val config = system.settings.config
    val port = config.getInt("gearpump.services.ws")
    val host = config.getString("gearpump.services.host")
    IO(UHttp) ! Http.Bind(services, interface = host, port = port)
  }
}

