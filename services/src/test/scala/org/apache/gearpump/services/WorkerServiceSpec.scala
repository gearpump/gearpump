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
import org.apache.gearpump.cluster.AppMasterToMaster.{GetWorkerData, WorkerData}
import org.apache.gearpump.cluster.ClientToMaster.{QueryWorkerConfig, ResolveWorkerId}
import org.apache.gearpump.cluster.MasterToClient.{ResolveWorkerIdResult, WorkerConfig}
import org.apache.gearpump.cluster.worker.WorkerDescription
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import spray.testkit.ScalatestRouteTest

import scala.concurrent.duration._
import scala.util.{Success, Try}

class WorkerServiceSpec extends FlatSpec with ScalatestRouteTest with WorkerService with Matchers with BeforeAndAfterAll {
  def actorRefFactory = system

  val mockWorker = TestProbe()

  mockWorker.setAutoPilot {
    new AutoPilot {
      def run(sender: ActorRef, msg: Any) = msg match {
        case GetWorkerData(appId) =>
          sender ! WorkerData(WorkerDescription.empty)
          KeepRunning
        case QueryWorkerConfig(appId) =>
          sender ! WorkerConfig(null)
          KeepRunning
      }
    }
  }

  val mockMaster = TestProbe()
  mockMaster.setAutoPilot {
    new AutoPilot {
      def run(sender: ActorRef, msg: Any) = msg match {
        case ResolveWorkerId(workerId) =>
          sender ! ResolveWorkerIdResult(Success(mockWorker.ref))
          KeepRunning
      }
    }
  }

  def master = mockMaster.ref

  "ConfigQueryService" should "return config for worker" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/worker/1/config") ~> workerRoute).asInstanceOf[RouteResult] ~> check{
      val responseBody = response.entity.asString
      val config = Try(ConfigFactory.parseString(responseBody))
      assert(config.isSuccess)
    }
  }

  it should "return WorkerData" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/worker/1") ~> workerRoute).asInstanceOf[RouteResult] ~> check{
      val responseBody = response.entity.asString
      val config = Try(ConfigFactory.parseString(responseBody))
      assert(config.isSuccess)
    }
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }
}
