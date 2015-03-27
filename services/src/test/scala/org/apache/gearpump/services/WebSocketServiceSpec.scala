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

import akka.actor.ActorRef
import com.typesafe.config.Config
import org.apache.gearpump.cluster.TestUtil
import org.apache.gearpump.cluster.TestUtil.MiniCluster
import org.apache.gearpump.services.WebSocketService.WebSocketUrl
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import spray.testkit.ScalatestRouteTest

import scala.concurrent.duration._

class WebSocketServiceSpec extends FlatSpec with ScalatestRouteTest with WebSocketService with Matchers with BeforeAndAfterAll {
  import upickle._
  def actorRefFactory = system
  implicit val config: Config = system.settings.config

  var miniCluster:MiniCluster = null
  implicit def master:ActorRef = miniCluster.mockMaster

  override def beforeAll: Unit = {
    miniCluster = TestUtil.startMiniCluster
  }

  override def afterAll: Unit = {
    miniCluster.shutDown()
  }

  "WebSocketService" should "return a json structure of WebSocket url" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/websocket/url") ~> webSocketRoute).asInstanceOf[RouteResult] ~> check {
      //check the type
      read[WebSocketUrl](response.entity.asString)
    }
  }
}
