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

package io.gearpump.services

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Route, _}
import akka.stream.ActorMaterializer
import io.gearpump.jarstore.JarStoreService
import io.gearpump.util.{Constants, LogUtil}
import org.apache.commons.lang.exception.ExceptionUtils

class RestServices(actorSystem: ActorSystem, val master: ActorRef) extends
    StaticService with
    MasterService with
    WorkerService with
    AppMasterService with
    JarStoreProvider {
  private val LOG = LogUtil.getLogger(getClass)

  implicit def system: ActorSystem = actorSystem

  private def myExceptionHandler: ExceptionHandler = ExceptionHandler{
    case ex: Throwable => {
      extractUri { uri =>
        LOG.error(s"Request to $uri could not be handled normally", ex)
        complete(InternalServerError, ExceptionUtils.getStackTrace(ex))
      }
    }
  }

  def routes = handleExceptions(myExceptionHandler) {
    masterRoute ~
    workerRoute ~
    appMasterRoute ~
    // make sure staticRoute is the final one, as it will try to lookup resource in local path
    // if there is no match in previous routes
    staticRoute
  }

  private val jarStoreService = JarStoreService.get(system.settings.config)
  jarStoreService.init(system.settings.config, system)

  override def getJarStoreService: JarStoreService = jarStoreService
}

trait JarStoreProvider {
  def getJarStoreService: JarStoreService
}

object RestServices {
  private val LOG = LogUtil.getLogger(getClass)

  def apply(master:ActorRef)(implicit system:ActorSystem) {
    implicit val executionContext = system.dispatcher
    val services = new RestServices(system, master)
    val config = system.settings.config
    val port = config.getInt(Constants.GEARPUMP_SERVICE_HTTP)
    val host = config.getString(Constants.GEARPUMP_SERVICE_HOST)

    implicit val materializer = ActorMaterializer()

    Http().bindAndHandle(Route.handlerFlow(services.routes), host, port)

    val displayHost = if(host == "0.0.0.0") "127.0.0.1" else host
    LOG.info(s"Please browse to http://$displayHost:$port to see the web UI")
    println(s"Please browse to http://$displayHost:$port to see the web UI")
  }
}