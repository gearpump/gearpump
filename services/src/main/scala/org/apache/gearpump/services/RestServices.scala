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

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.io.IO
import com.typesafe.config.Config
import org.apache.gearpump.util.LogUtil
import spray.can._
import spray.routing.RoutingSettings

import scala.concurrent.ExecutionContextExecutor

trait RestServices extends AppMastersService
    with AppMasterService with WorkerService with WorkersService with MasterService
    with ConfigQueryService with MetricsQueryService with WebSocketService with StaticService {
  implicit def executionContext: ExecutionContextExecutor = actorRefFactory.dispatcher

  lazy val routes = appMastersRoute ~ appMasterRoute ~ workersRoute ~ workerRoute ~
    masterRoute ~  configQueryRoute ~ metricQueryRoute ~ webSocketRoute ~ staticRoute
}

class RestServicesActor(masters: ActorRef, sys:ActorSystem, conf:Config) extends Actor with RestServices {
  def actorRefFactory = context
  implicit val system: ActorSystem = sys
  implicit val eh = RoutingSettings.default(context)
  implicit val config: Config = conf
  implicit val master: ActorRef = masters
  def receive = runRoute(routes)
}

object RestServices {
  private val LOG = LogUtil.getLogger(getClass)

  def apply(master:ActorRef, config: Config)(implicit system:ActorSystem) {
    implicit val executionContext = system.dispatcher
    val services = system.actorOf(Props(classOf[RestServicesActor], master, system, config), "rest-services")
    val port = config.getInt("gearpump.services.http")
    val host = config.getString("gearpump.services.host")
    IO(Http) ! Http.Bind(services, interface = host, port = port)

    LOG.info(s"Please browse to http://$host:$port to see the web UI")
    println(s"Please browse to http://$host:$port to see the web UI")
  }
}