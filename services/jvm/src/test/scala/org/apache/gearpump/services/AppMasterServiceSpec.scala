/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.services

import scala.concurrent.duration._
import scala.util.{Success, Try}
import akka.actor.ActorRef
import akka.http.scaladsl.model.headers.`Cache-Control`
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit.TestActor.{AutoPilot, KeepRunning}
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.slf4j.Logger
import upickle.default.read
import org.apache.gearpump.cluster.AppMasterToMaster.GeneralAppMasterSummary
import org.apache.gearpump.cluster.ClientToMaster.{GetLastFailure, QueryAppMasterConfig, QueryHistoryMetrics, ResolveAppId}
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMasterDataDetailRequest, AppMasterDataRequest}
import org.apache.gearpump.cluster.MasterToClient._
import org.apache.gearpump.cluster.{ApplicationStatus, TestUtil}
import org.apache.gearpump.jarstore.{JarStoreClient, JarStoreServer}
import org.apache.gearpump.streaming.executor.Executor.{ExecutorConfig, ExecutorSummary, GetExecutorSummary, QueryExecutorConfig}
import org.apache.gearpump.util.LogUtil
// NOTE: This cannot be removed!!!
import org.apache.gearpump.services.util.UpickleUtil._

class AppMasterServiceSpec extends FlatSpec with ScalatestRouteTest
  with Matchers with BeforeAndAfterAll {

  override def testConfig: Config = TestUtil.UI_CONFIG

  val mockAppMaster = TestProbe()
  val failure = LastFailure(System.currentTimeMillis(), "Some error")
  val jarStoreClient = new JarStoreClient(system.settings.config, system)

  private def master = mockMaster.ref

  private def appMasterRoute = new AppMasterService(master, jarStoreClient, system).route

  mockAppMaster.setAutoPilot {
    new AutoPilot {
      def run(sender: ActorRef, msg: Any): AutoPilot = msg match {
        case AppMasterDataDetailRequest(appId) =>
          sender ! GeneralAppMasterSummary(appId)
          KeepRunning
        case QueryHistoryMetrics(path, _, _, _) =>
          sender ! HistoryMetrics(path, List.empty[HistoryMetricsItem])
          KeepRunning
        case GetLastFailure(appId) =>
          sender ! failure
          KeepRunning
        case GetExecutorSummary(0) =>
          sender ! ExecutorSummary.empty
          KeepRunning
        case QueryExecutorConfig(0) =>
          sender ! ExecutorConfig(system.settings.config)
          KeepRunning
      }
    }
  }

  val mockMaster = TestProbe()
  mockMaster.setAutoPilot {
    new AutoPilot {
      def run(sender: ActorRef, msg: Any): AutoPilot = msg match {
        case ResolveAppId(0) =>
          sender ! ResolveAppIdResult(Success(mockAppMaster.ref))
          KeepRunning
        case AppMasterDataRequest(appId, _) =>
          sender ! AppMasterData(ApplicationStatus.ACTIVE)
          KeepRunning
        case QueryAppMasterConfig(appId) =>
          sender ! AppMasterConfig(null)
          KeepRunning
      }
    }
  }

  "AppMasterService" should "return a JSON structure for GET request when detail = false" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    Get(s"/api/$REST_VERSION/appmaster/0?detail=false") ~> appMasterRoute ~> check {
      val responseBody = responseAs[String]
      read[AppMasterData](responseBody)

      // Checks the header, should contains no-cache header.
      // Cache-Control:no-cache, max-age=0
      val noCache = header[`Cache-Control`].get.value()
      assert(noCache == "no-cache, max-age=0")
    }

    Get(s"/api/$REST_VERSION/appmaster/0?detail=true") ~> appMasterRoute ~> check {
      val responseBody = responseAs[String]
    }
  }

  "MetricsQueryService" should "return history metrics" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/appmaster/0/metrics/processor") ~> appMasterRoute) ~> check {
      val responseBody = responseAs[String]
      val config = Try(ConfigFactory.parseString(responseBody))
      assert(config.isSuccess)
    }
  }

  "AppMaster" should "return lastest error" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/appmaster/0/errors") ~> appMasterRoute) ~> check {
      val responseBody = responseAs[String]
      assert(read[LastFailure](responseBody) == failure)
    }
  }

  "ConfigQueryService" should "return config for application" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/appmaster/0/config") ~> appMasterRoute) ~> check {
      val responseBody = responseAs[String]
      val config = Try(ConfigFactory.parseString(responseBody))
      assert(config.isSuccess)
    }
  }

  it should "return config for executor " in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/appmaster/0/executor/0/config") ~> appMasterRoute) ~> check {
      val responseBody = responseAs[String]
      val config = Try(ConfigFactory.parseString(responseBody))
      assert(config.isSuccess)
    }
  }

  it should "return return executor summary" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/appmaster/0/executor/0") ~> appMasterRoute) ~> check {
      val responseBody = responseAs[String]
      val executorSummary = read[ExecutorSummary](responseBody)
      assert(executorSummary.id == 0)
    }
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }
}
