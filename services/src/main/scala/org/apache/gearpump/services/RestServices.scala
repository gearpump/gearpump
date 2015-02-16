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
import org.apache.gearpump.util.LogUtil
import spray.can._
import spray.routing.RoutingSettings

trait RestServices extends AppMastersService
    with AppMasterService with WorkerService with WorkersService with MasterService with StaticService {
  implicit def executionContext = actorRefFactory.dispatcher

  lazy val route = appMastersRoute ~ appMasterRoute ~ workersRoute ~ workerRoute ~ masterRoute ~ staticRoute
}

class RestServicesActor(masters: ActorRef, sys:ActorSystem) extends Actor with RestServices {
  def actorRefFactory = context
  implicit val system: ActorSystem = sys
  implicit val eh = RoutingSettings.default(context)

  val master = masters
  def receive = runRoute(route)
}

object RestServices {
  def start(master:ActorRef)(implicit system:ActorSystem) {
    implicit val executionContext = system.dispatcher
    val services = system.actorOf(Props(classOf[RestServicesActor], master, system), "rest-services")
    val config = system.settings.config
    val port = config.getInt("gearpump.services.http")
    val host = config.getString("gearpump.services.host")
    IO(Http) ! Http.Bind(services, interface = host, port = port)
  }
}