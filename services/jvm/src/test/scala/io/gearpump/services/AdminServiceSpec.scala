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

import akka.actor.ActorSystem
import io.gearpump.cluster.TestUtil
import akka.http.scaladsl.testkit.{ScalatestRouteTest, RouteTestTimeout}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}

import scala.concurrent.duration._

import scala.util.Try

class AdminServiceSpec  extends FlatSpec with ScalatestRouteTest with Matchers with BeforeAndAfterAll {

  implicit var actorSystem: ActorSystem = null

  override def beforeAll() = {
    actorSystem = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
  }

  override def afterAll() = {
    actorSystem.shutdown()
    actorSystem.awaitTermination()
  }

  it should "shutdown the ActorSystem when receiving terminate" in {
    val route = new AdminService(actorSystem).route
    implicit val customTimeout = RouteTestTimeout(15 seconds)
    (Post(s"/terminate") ~> route) ~> check{
      assert(status.intValue() == 404)
    }

    actorSystem.awaitTermination(20 seconds)

    // terminate should terminate current actor system
    assert(actorSystem.isTerminated)
  }
}
