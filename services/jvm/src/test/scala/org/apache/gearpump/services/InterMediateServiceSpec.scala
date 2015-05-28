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

import akka.actor.{ActorRef, Props, Actor, ActorSystem}
import akka.io.IO
import org.apache.gearpump.util.Constants
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import spray.can.Http
import spray.routing.{RoutingSettings, HttpService}
import spray.testkit.ScalatestRouteTest

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class InterMediateServiceSpec extends FlatSpec with ScalatestRouteTest
  with InterMediateService with Matchers with BeforeAndAfterAll {
  def actorRefFactory = system

  def master = TestCluster.master

  InterMediateServiceSpec.startMockRestService(system)

  "ConfigQueryService" should "return config for application" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Post(s"/api/$REST_VERSION/internal/intermediate/delete/test/intermediate") ~> interMediateRoute).asInstanceOf[RouteResult] ~> check{
      val responseBody = response.entity.asString
      assert(responseBody == "delete succeed")
    }

    (Post(s"/api/$REST_VERSION/internal/intermediate/put/test/intermediate") ~> interMediateRoute).asInstanceOf[RouteResult] ~> check{
      val responseBody = response.entity.asString
      assert(responseBody == "put succeed")
    }
  }
}

object InterMediateServiceSpec {
  def startMockRestService(implicit system: ActorSystem) = {
    implicit val executionContext = system.dispatcher
    val services = system.actorOf(Props(classOf[MockRestServiceActor], system), "rest-services")
    val config = system.settings.config
    val port = config.getInt(Constants.GEARPUMP_SERVICE_HTTP)
    val host = config.getString(Constants.GEARPUMP_SERVICE_HOST)
    IO(Http) ! Http.Bind(services, interface = host, port = port)
  }

  class MockRestServiceActor(sys: ActorSystem) extends Actor with HttpService{
    def actorRefFactory = context
    implicit val system: ActorSystem = sys
    implicit val eh = RoutingSettings.default(context)
    def receive = runRoute(routes)

    def routes = {
      implicit val ec: ExecutionContext = actorRefFactory.dispatcher
      implicit val timeout = Constants.FUTURE_TIMEOUT
      pathPrefix("api" / s"$REST_VERSION") {
        path("test" / "intermediate") {
          delete {
            complete("delete succeed")
          } ~
            put {
              complete("put succeed")
            }
        }
      }
    }
  }
}