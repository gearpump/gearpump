/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.services

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit.{TestKit, TestProbe}
import akka.testkit.TestActor.{AutoPilot, KeepRunning}
import com.typesafe.config.{Config, ConfigFactory}
import io.gearpump.cluster.AppMasterToMaster.{GetWorkerData, WorkerData}
import io.gearpump.cluster.ClientToMaster._
import io.gearpump.cluster.MasterToClient.ResolveWorkerIdResult
import io.gearpump.cluster.TestUtil
import io.gearpump.cluster.worker.{WorkerId, WorkerSummary}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Ignore, Matchers}
import scala.concurrent.duration._
import scala.util.Success

@Ignore
class SupervisorServiceSpec
  extends FlatSpec with ScalatestRouteTest with Matchers with BeforeAndAfterAll {

  override def testConfig: Config = TestUtil.DEFAULT_CONFIG

  protected def actorRefFactory = system

  private val mockSupervisor = TestProbe()

  private val supervisor = mockSupervisor.ref

  private val mockMaster = TestProbe()

  protected def master = mockMaster.ref

  private val mockWorker = TestProbe()

  protected def supervisorRoute = new SupervisorService(master, supervisor, system).route

  protected def nullRoute = new SupervisorService(master, null, system).route

  mockSupervisor.setAutoPilot {
    new AutoPilot {
      def run(sender: ActorRef, msg: Any): AutoPilot = msg match {
        case AddWorker(_) =>
          sender ! CommandResult(success = true)
          KeepRunning
        case RemoveWorker(_) =>
          sender ! CommandResult(success = true)
          KeepRunning
      }
    }
  }

  mockWorker.setAutoPilot {
    new AutoPilot {
      def run(sender: ActorRef, msg: Any): AutoPilot = msg match {
        case GetWorkerData(_) =>
          sender ! WorkerData(WorkerSummary.empty)
          KeepRunning
      }
    }
  }

  mockMaster.setAutoPilot {
    new AutoPilot {
      def run(sender: ActorRef, msg: Any): AutoPilot = msg match {
        case ResolveWorkerId(_) =>
          sender ! ResolveWorkerIdResult(Success(mockWorker.ref))
          KeepRunning
      }
    }
  }

  "SupervisorService" should "get supervisor path" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/supervisor") ~> supervisorRoute) ~> check {
      val responseBody = responseAs[String]
      ConfigFactory.parseString(responseBody).getString("path") shouldBe supervisor.path.toString
    }

    (Get(s"/api/$REST_VERSION/supervisor") ~> nullRoute) ~> check {
      val responseBody = responseAs[String]
      ConfigFactory.parseString(responseBody).getIsNull("path") shouldBe true
    }
  }

  "SupervisorService" should "write status" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Post(s"/api/$REST_VERSION/supervisor/status")
      ~> supervisorRoute) ~> check {
      val responseBody = responseAs[String]
      ConfigFactory.parseString(responseBody).getBoolean("enabled") shouldBe true
    }

    (Post(s"/api/$REST_VERSION/supervisor/status") ~> nullRoute) ~> check {
      val responseBody = responseAs[String]
      ConfigFactory.parseString(responseBody).getBoolean("enabled") shouldBe false
    }
  }

  "SupervisorService" should "add worker" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Post(s"/api/$REST_VERSION/supervisor/addworker/1")
      ~> supervisorRoute) ~> check {
      val responseBody = responseAs[String]
      ConfigFactory.parseString(responseBody).getBoolean("success") shouldBe true
    }

    (Post(s"/api/$REST_VERSION/supervisor/addworker/1")
      ~> nullRoute) ~> check {
      status shouldBe StatusCodes.InternalServerError
    }
  }

  "SupervisorService" should "remove worker" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Post(s"/api/$REST_VERSION/supervisor/removeworker/${WorkerId.render(WorkerId(1, 0L))}")
      ~> supervisorRoute) ~> check {
      val responseBody = responseAs[String]
      ConfigFactory.parseString(responseBody).getBoolean("success") shouldBe true
    }


    (Post(s"/api/$REST_VERSION/supervisor/removeworker/${WorkerId.render(WorkerId(1, 0L))}")
      ~> nullRoute) ~> check {
      status shouldBe StatusCodes.InternalServerError
    }
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }
}
