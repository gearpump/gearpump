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

import akka.actor.{ActorRef, Actor, ActorSystem, Props}
import akka.io.IO
import com.gettyimages.spray.swagger._
import com.typesafe.config.ConfigFactory
import com.wordnik.swagger.model.ApiInfo
import spray.can._
import spray.json._
import spray.routing.HttpService

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._

class RestServices(master: ActorRef)(implicit executionContext:ExecutionContext) extends Actor with HttpService with DefaultJsonProtocol {

  def actorRefFactory = context

  val appMasterService = new AppMasterService(master, context, executionContext)

  def receive = runRoute(appMasterService.routes ~ swaggerService.routes ~
      get {
        pathPrefix("") { 
          pathEndOrSingleSlash {
            getFromResource("swagger-ui/index.html")
          }
        } ~
        getFromResourceDirectory("swagger-ui")
  })

  val swaggerService = new SwaggerHttpService {
    override def apiTypes = Seq(typeOf[AppMasterService])
    override def apiVersion = "2.0"
    override def baseUrl = "/"
    override def docsPath = "api-docs"
    override def actorRefFactory = context
    override def apiInfo = Some(new ApiInfo(title="Spray-Swagger", description="A service using spray and spray-swagger.", termsOfServiceUrl="", contact="", "Apache V2", "http://www.apache.org/licenses/LICENSE-2.0"))

    //authorizations, not used
  }
}

object RestServices {
  def start(master:ActorRef)(implicit system:ActorSystem) {
    implicit val executionContext = system.dispatcher
    val services = system.actorOf(Props(new RestServices(master)), "rest-services")
    val config = ConfigFactory.load()
    val port = config.getInt("gearpump.rest-services.port")
    val host = config.getString("gearpump.rest-services.host")
    IO(Http) ! Http.Bind(services, interface = host, port = port)
  }
}
