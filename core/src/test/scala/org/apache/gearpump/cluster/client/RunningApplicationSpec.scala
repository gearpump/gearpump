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

package org.apache.gearpump.cluster.client

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import akka.util.Timeout
import org.apache.gearpump.cluster.ClientToMaster.{ResolveAppId, ShutdownApplication}
import org.apache.gearpump.cluster.MasterToClient.{ResolveAppIdResult, ShutdownApplicationResult}
import org.apache.gearpump.cluster.TestUtil
import org.apache.gearpump.cluster.client.RunningApplicationSpec.{MockAskAppMasterRequest, MockAskAppMasterResponse}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

class RunningApplicationSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit var system: ActorSystem = _

  override def beforeAll(): Unit = {
    system = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
  }

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }

  "RunningApplication" should "be able to shutdown application" in {
    val errorMsg = "mock exception"
    val master = TestProbe()
    val timeout = Timeout(90, TimeUnit.SECONDS)
    val application = new RunningApplication(1, master.ref, timeout)
    Future {
      application.shutDown()
    }
    master.expectMsg(ShutdownApplication(1))
    master.reply(ShutdownApplicationResult(Success(1)))

    val result = Future {
      intercept[Exception] {
        application.shutDown()
      }
    }
    master.expectMsg(ShutdownApplication(1))
    master.reply(ShutdownApplicationResult(Failure(new Exception(errorMsg))))
    val exception = Await.result(result, Duration.Inf)
    assert(exception.getMessage.equals(errorMsg))
  }

  "RunningApplication" should "be able to ask appmaster" in {
    val master = TestProbe()
    val appMaster = TestProbe()
    val appId = 1
    val timeout = Timeout(90, TimeUnit.SECONDS)
    val request = MockAskAppMasterRequest("request")
    val application = new RunningApplication(appId, master.ref, timeout)
    val future = application.askAppMaster[MockAskAppMasterResponse](request)
    master.expectMsg(ResolveAppId(appId))
    master.reply(ResolveAppIdResult(Success(appMaster.ref)))
    appMaster.expectMsg(MockAskAppMasterRequest("request"))
    appMaster.reply(MockAskAppMasterResponse("response"))
    val result = Await.result(future, Duration.Inf)
    assert(result.res.equals("response"))

    // ResolveAppId should not be called multiple times
    val future2 = application.askAppMaster[MockAskAppMasterResponse](request)
    appMaster.expectMsg(MockAskAppMasterRequest("request"))
    appMaster.reply(MockAskAppMasterResponse("response"))
    val result2 = Await.result(future2, Duration.Inf)
    assert(result2.res.equals("response"))
  }
}

object RunningApplicationSpec {
  case class MockAskAppMasterRequest(req: String)

  case class MockAskAppMasterResponse(res: String)
}