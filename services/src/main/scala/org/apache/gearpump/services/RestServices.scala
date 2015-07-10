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
import org.apache.gearpump.util.{Constants, LogUtil}
import spray.can._
import spray.routing.RoutingSettings

trait RestServices extends
    StaticService with
    MasterService with
    WorkerService with
    AppMasterService {
  implicit def executionContext = actorRefFactory.dispatcher

  lazy val routes =
    masterRoute ~
    workerRoute ~
    appMasterRoute ~
    // make sure staticRoute is the final one, as it will try to lookup resource in local path
    // if there is no match in previous routes
    staticRoute
}

class RestServicesActor(masters: ActorRef, sys:ActorSystem) extends Actor with RestServices {
  def actorRefFactory = context
  implicit val system: ActorSystem = sys
  implicit val eh = RoutingSettings.default(context)

  val master = masters
  def receive = runRoute(routes)
}

object RestServices {
  private val LOG = LogUtil.getLogger(getClass)

  def apply(master:ActorRef)(implicit system:ActorSystem) {
    implicit val executionContext = system.dispatcher
    val services = system.actorOf(Props(classOf[RestServicesActor], master, system), "rest-services")
    val config = system.settings.config
    val port = config.getInt(Constants.GEARPUMP_SERVICE_HTTP)
    val host = config.getString(Constants.GEARPUMP_SERVICE_HOST)
    IO(Http) ! Http.Bind(services, interface = host, port = port)

    val displayHost = if(host == "0.0.0.0") "127.0.0.1" else host
    LOG.info(s"Please browse to http://$displayHost:$port to see the web UI")
    println(s"Please browse to http://$displayHost:$port to see the web UI")
  }
}