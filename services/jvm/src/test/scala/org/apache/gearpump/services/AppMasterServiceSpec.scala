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
import akka.testkit.TestActor.{AutoPilot, KeepRunning}
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.AppMasterToMaster.GeneralAppMasterSummary
import org.apache.gearpump.cluster.ClientToMaster.{GetLastFailure, QueryAppMasterConfig, QueryHistoryMetrics, ResolveAppId}
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMasterDataDetailRequest, AppMasterDataRequest}
import org.apache.gearpump.cluster.MasterToClient._
import org.apache.gearpump.streaming.executor.Executor.{ExecutorConfig, QueryExecutorConfig, ExecutorSummary, GetExecutorSummary}
import org.apache.gearpump.util.LogUtil
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.slf4j.Logger
import spray.testkit.ScalatestRouteTest

import scala.concurrent.duration._
import scala.util.{Success, Try}
import upickle.default.{read,write}

class AppMasterServiceSpec extends FlatSpec with ScalatestRouteTest with AppMasterService with Matchers with BeforeAndAfterAll {

  private val LOG: Logger = LogUtil.getLogger(getClass)
  def actorRefFactory = system

  val mockAppMaster = TestProbe()
  val failure = LastFailure(System.currentTimeMillis(), "Some error")

  mockAppMaster.setAutoPilot {
    new AutoPilot {
      def run(sender: ActorRef, msg: Any) = msg match {
        case AppMasterDataDetailRequest(appId) =>
          sender ! GeneralAppMasterSummary(appId)
          KeepRunning
        case QueryHistoryMetrics(appId, path, _) =>
          sender ! HistoryMetrics(appId, path, List.empty[HistoryMetricsItem])
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
      def run(sender: ActorRef, msg: Any) = msg match {
        case ResolveAppId(0) =>
          sender ! ResolveAppIdResult(Success(mockAppMaster.ref))
          KeepRunning
        case AppMasterDataRequest(appId, _) =>
          sender ! AppMasterData("active")
          KeepRunning
        case QueryAppMasterConfig(appId) =>
          sender ! AppMasterConfig(null)
          KeepRunning
      }
    }
  }

  def master = mockMaster.ref

  "AppMasterService" should "return a JSON structure for GET request when detail = false" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    Get(s"/api/$REST_VERSION/appmaster/0?detail=false") ~> appMasterRoute ~> check{
      val responseBody = response.entity.asString
      read[AppMasterData](responseBody)
    }

    Get(s"/api/$REST_VERSION/appmaster/0?detail=true") ~> appMasterRoute ~> check{
      val responseBody = response.entity.asString
    }

  }

  "MetricsQueryService" should "return history metrics" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/appmaster/0/metrics/processor") ~> appMasterRoute).asInstanceOf[RouteResult] ~> check {
      val responseBody = response.entity.asString
      val config = Try(ConfigFactory.parseString(responseBody))
      assert(config.isSuccess)
    }
  }

  "AppMaster" should "return lastest error" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/appmaster/0/errors") ~> appMasterRoute).asInstanceOf[RouteResult] ~> check {
      val responseBody = response.entity.asString
      assert(read[LastFailure](responseBody) == failure)
    }
  }

  "ConfigQueryService" should "return config for application" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/appmaster/0/config") ~> appMasterRoute).asInstanceOf[RouteResult] ~> check{
      val responseBody = response.entity.asString
      val config = Try(ConfigFactory.parseString(responseBody))
      assert(config.isSuccess)
    }
  }

  it should "return config for executor " in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/appmaster/0/executor/0/config") ~> appMasterRoute).asInstanceOf[RouteResult] ~> check{
      val responseBody = response.entity.asString
      val config = Try(ConfigFactory.parseString(responseBody))
      assert(config.isSuccess)
    }
  }

  it should "return return executor summary" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/appmaster/0/executor/0") ~> appMasterRoute).asInstanceOf[RouteResult] ~> check{
      val responseBody = response.entity.asString
      val executorSummary = read[ExecutorSummary](responseBody)
      assert(executorSummary.id == 0)
    }
  }


  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }
}
