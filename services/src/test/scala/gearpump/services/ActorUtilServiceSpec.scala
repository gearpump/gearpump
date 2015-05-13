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

package gearpump.services

import gearpump.streaming.StreamingTestUtil
import gearpump.cluster.MasterToAppMaster.AppMasterData
import gearpump.cluster.TestUtil
import gearpump.cluster.TestUtil.MiniCluster
import gearpump.util.LogUtil
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.slf4j.Logger
import spray.testkit.ScalatestRouteTest

import scala.concurrent.duration._

class ActorUtilServiceSpec extends FlatSpec with ScalatestRouteTest
with ActorUtilService with Matchers  {
  import upickle._
  private val LOG: Logger = LogUtil.getLogger(getClass)
  def actorRefFactory = system

  def master = TestCluster.master

  "ActorUtilService" should "deliver message to target actor path" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/internal/actorutil?" +
      "class=gearpump.streaming.task.SendMessageLoss&actor=/test/path")
      ~> actorUtilRoute).asInstanceOf[RouteResult] ~> check{

      assert(response.status.isSuccess)
    }
  }
}
