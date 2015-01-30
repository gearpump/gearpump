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
import org.apache.gearpump.cluster.AppMasterToMaster.AppMasterDataDetail
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.streaming.{AppDescription, TaskDescription, DAG}
import org.apache.gearpump.util.Graph
import spray.can._

import scala.concurrent.ExecutionContext

trait RestServices extends AppMastersService with AppMasterService {
  val route = appMastersRoute ~ appMasterRoute
}

class RestServicesActor(masters: ActorRef) extends Actor with RestServices {
  val master = masters
  def actorRefFactory = context
  implicit val system: ActorSystem = context.system
  implicit val executionContext: ExecutionContext = context.dispatcher

  def receive = runRoute(route)
}

object RestServices {
  def start(master:ActorRef)(implicit system:ActorSystem) {
    implicit val executionContext = system.dispatcher
    val services = system.actorOf(Props(classOf[RestServicesActor], master), "rest-services")
    val config = system.settings.config
    val port = config.getInt("gearpump.rest-services.port")
    val host = config.getString("gearpump.rest-services.host")
    IO(Http) ! Http.Bind(services, interface = host, port = port)
  }
}
