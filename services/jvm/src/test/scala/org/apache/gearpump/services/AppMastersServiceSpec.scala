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


import org.apache.gearpump.cluster.MasterToAppMaster.AppMastersData
import org.apache.gearpump.util.LogUtil
import org.scalatest._
import org.slf4j.Logger
import spray.testkit.ScalatestRouteTest

import scala.concurrent.duration._


class AppMastersServiceSpec extends FlatSpec with ScalatestRouteTest with AppMastersService with Matchers with BeforeAndAfterAll  {
  import upickle._
  private val LOG: Logger = LogUtil.getLogger(getClass)
  def actorRefFactory = system
  val testCluster = TestCluster(system)

  def master = testCluster.master

  "AppMastersService" should "return a json structure of appMastersData for GET request" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/appmasters") ~> appMastersRoute).asInstanceOf[RouteResult] ~> check {
      //check the type
      read[AppMastersData](response.entity.asString)
    }
  }

  override def afterAll {
    testCluster.shutDown
  }
}