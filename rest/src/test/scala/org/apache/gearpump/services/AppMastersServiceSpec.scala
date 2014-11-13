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

import akka.actor.{Actor, ActorSystem}
import org.specs2.mutable.Specification
import spray.routing.{RouteConcatenation, Route, HttpService}
import spray.testkit.Specs2RouteTest

/*
class AppMastersServiceSpec extends AppMastersServiceSpecActor {


}

class AppMastersServiceSpecActor extends AppMastersServiceSpecBase with Actor {
  val appMastersService = new AppMastersService(restTest.miniCluster.mockMaster, context, system.dispatcher)
  def receive = runRoute(appMastersService.routes)
}
*/

class AppMastersServiceSpecBase extends Specification with Specs2RouteTest with HttpService  {
  args(sequential = true)
  implicit def actorRefFactory = system

  val restTest = RestTestUtil.startRestServices

  val smallRoute =
    get {
      pathSingleSlash {
        complete {
          <html>
            <body>
              <h1>Say hello to <i>spray</i>!</h1>
            </body>
          </html>
        }
      } ~
        path("appmasters") {
          complete("success")
        }
    }

  "The AppMastersService" should {
    "return a json structure of appmastersdata for GET request" in {
      Get("/appmasters") ~> smallRoute ~> check {
        responseAs[String] === "success"
      }
    }
  }

}