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

import org.apache.gearpump.shared.Messages.MasterData
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import spray.testkit.ScalatestRouteTest

import scala.concurrent.duration._

class MasterServiceSpec extends FlatSpec with ScalatestRouteTest with MasterService with Matchers with BeforeAndAfterAll {

  import upickle._

  def actorRefFactory = system
  val testCluster = TestCluster(system)

  def master = testCluster.master

  it should "return master info when asked" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/master") ~> masterRoute).asInstanceOf[RouteResult] ~> check {
      // check the type
      val content = response.entity.asString
      read[MasterData](content)
    }
  }

  override def afterAll {
    testCluster.shutDown
  }
}
