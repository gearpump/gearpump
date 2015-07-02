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

import java.io.File

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.apache.gearpump.util.Constants
import org.scalatest._
import spray.http._

import spray.testkit.ScalatestRouteTest

class SubmitUserApplicationServiceSpec(sys: ActorSystem) extends FlatSpec
with ScalatestRouteTest
with SubmitUserApplicationService
with Matchers
with BeforeAndAfterAll {

  def this() = this(ActorSystem("MySpec"))

  override implicit val system: ActorSystem = sys

  def actorRefFactory = system

  "SubmitUserApplicationServiceSpec" should "submit an invalid jar and get success = false" in {
    implicit val timeout = Constants.FUTURE_TIMEOUT
    val tempfile = new File("foo")
    val mfd = MultipartFormData(Seq(BodyPart(tempfile, "file")))
    Post(s"/api/$REST_VERSION/userapp/submit", mfd) ~> submitUserApplicationRoute ~> check {
      import upickle._
      val actual = read[SubmitUserApplicationService.Status](response.entity.asString)
      actual.success shouldEqual false
    }
  }

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }
}
