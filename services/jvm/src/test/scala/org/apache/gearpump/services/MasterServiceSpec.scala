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

import java.io.File
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

import akka.actor.ActorRef
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.`Cache-Control`
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.stream.scaladsl.{FileIO, Source}
import akka.testkit.TestActor.{AutoPilot, KeepRunning}
import akka.testkit.TestProbe
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import org.apache.gearpump.cluster.AppMasterToMaster.{GetAllWorkers, GetMasterData, GetWorkerData, MasterData, WorkerData}
import org.apache.gearpump.cluster.ClientToMaster.{QueryHistoryMetrics, QueryMasterConfig, ResolveWorkerId, SubmitApplication}
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMastersData, AppMastersDataRequest, WorkerList}
import org.apache.gearpump.cluster.MasterToClient._
import org.apache.gearpump.cluster.TestUtil
import org.apache.gearpump.cluster.worker.{WorkerId, WorkerSummary}
import org.apache.gearpump.jarstore.{JarStoreClient, JarStoreServer}
import org.apache.gearpump.services.MasterService.{BuiltinPartitioners, SubmitApplicationRequest}
// NOTE: This cannot be removed!!!
import org.apache.gearpump.services.util.UpickleUtil._
import org.apache.gearpump.streaming.ProcessorDescription
import org.apache.gearpump.util.Graph

class MasterServiceSpec extends FlatSpec with ScalatestRouteTest
  with Matchers with BeforeAndAfterAll {
  import upickle.default.{read, write}

  override def testConfig: Config = TestUtil.UI_CONFIG

  val workerId = 0
  val mockWorker = TestProbe()

  val jarStoreClient = new JarStoreClient(system.settings.config, system)
  private def master = mockMaster.ref

  private def masterRoute = new MasterService(master, jarStoreClient, system).route

  mockWorker.setAutoPilot {
    new AutoPilot {
      def run(sender: ActorRef, msg: Any): AutoPilot = msg match {
        case GetWorkerData(workerId) =>
          sender ! WorkerData(WorkerSummary.empty.copy(state = "active", workerId = workerId))
          KeepRunning
        case QueryHistoryMetrics(path, _, _, _) =>
          sender ! HistoryMetrics(path, List.empty[HistoryMetricsItem])
          KeepRunning
      }
    }
  }

  val mockMaster = TestProbe()
  mockMaster.setAutoPilot {
    new AutoPilot {
      def run(sender: ActorRef, msg: Any): AutoPilot = msg match {
        case GetMasterData =>
          sender ! MasterData(null)
          KeepRunning
        case AppMastersDataRequest =>
          sender ! AppMastersData(List.empty[AppMasterData])
          KeepRunning
        case GetAllWorkers =>
          sender ! WorkerList(List(WorkerId(0, 0L)))
          KeepRunning
        case ResolveWorkerId(WorkerId(0, 0L)) =>
          sender ! ResolveWorkerIdResult(Success(mockWorker.ref))
          KeepRunning
        case QueryHistoryMetrics(path, _, _, _) =>
          sender ! HistoryMetrics(path, List.empty[HistoryMetricsItem])
          KeepRunning
        case QueryMasterConfig =>
          sender ! MasterConfig(null)
          KeepRunning
        case submit: SubmitApplication =>
          sender ! SubmitApplicationResult(Success(0))
          KeepRunning
      }
    }
  }

  it should "return master info when asked" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/master") ~> masterRoute) ~> check {
      // Checks the type
      val content = responseAs[String]
      read[MasterData](content)

      // Checks the header, should contains no-cache header.
      // Cache-Control:no-cache, max-age=0
      val noCache = header[`Cache-Control`].get.value()
      assert(noCache == "no-cache, max-age=0")
    }

    mockMaster.expectMsg(GetMasterData)
  }

  it should "return a json structure of appMastersData for GET request" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/master/applist") ~> masterRoute) ~> check {
      // Checks the type
      read[AppMastersData](responseAs[String])
    }
    mockMaster.expectMsg(AppMastersDataRequest)
  }

  it should "return a json structure of worker data for GET request" in {
    implicit val customTimeout = RouteTestTimeout(25.seconds)
    Get(s"/api/$REST_VERSION/master/workerlist") ~> masterRoute ~> check {
      // Checks the type
      val workerListJson = responseAs[String]
      val workers = read[List[WorkerSummary]](workerListJson)
      assert(workers.size > 0)
      workers.foreach { worker =>
        worker.state shouldBe "active"
      }
    }
    mockMaster.expectMsg(GetAllWorkers)
    mockMaster.expectMsgType[ResolveWorkerId]
    mockWorker.expectMsgType[GetWorkerData]
  }

  it should "return config for master" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/master/config") ~> masterRoute) ~> check {
      val responseBody = responseAs[String]
      val config = Try(ConfigFactory.parseString(responseBody))
      assert(config.isSuccess)
    }
    mockMaster.expectMsg(QueryMasterConfig)
  }

  "submit invalid application" should "return an error" in {
    implicit val routeTestTimeout = RouteTestTimeout(30.second)
    val tempfile = new File("foo")
    val request = entity(tempfile)

    Post(s"/api/$REST_VERSION/master/submitapp", request) ~> masterRoute ~> check {
      assert(response.status.intValue == 500)
    }
  }

  private def entity(file: File)(implicit ec: ExecutionContext): Future[RequestEntity] = {
    val entity = HttpEntity(MediaTypes.`application/octet-stream`, file.length(),
      FileIO.fromFile(file, chunkSize = 100000))

    val body = Source.single(
      Multipart.FormData.BodyPart(
        "file",
        entity,
        Map("filename" -> file.getName)))
    val form = Multipart.FormData(body)

    Marshal(form).to[RequestEntity]
  }

  "MetricsQueryService" should "return history metrics" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    (Get(s"/api/$REST_VERSION/master/metrics/master") ~> masterRoute) ~> check {
      val responseBody = responseAs[String]
      val config = Try(ConfigFactory.parseString(responseBody))
      assert(config.isSuccess)
    }
  }

  "submitDag" should "submit a SubmitApplicationRequest and get an appId > 0" in {
    import org.apache.gearpump.util.Graph._
    val processors = Map(
      0 -> ProcessorDescription(0, "A", parallelism = 1),
      1 -> ProcessorDescription(1, "B", parallelism = 1)
    )
    val dag = Graph(0 ~ "partitioner" ~> 1)
    val jsonValue = write(SubmitApplicationRequest("complexdag", processors, dag, null))
    Post(s"/api/$REST_VERSION/master/submitdag",
      HttpEntity(ContentTypes.`application/json`, jsonValue)) ~> masterRoute ~> check {
      val responseBody = responseAs[String]
      val submitApplicationResultValue = read[SubmitApplicationResultValue](responseBody)
      assert(submitApplicationResultValue.appId >= 0, "invalid appid")
    }
  }

  "MasterService" should "return Gearpump built-in partitioner list" in {
    (Get(s"/api/$REST_VERSION/master/partitioners") ~> masterRoute) ~> check {
      val responseBody = responseAs[String]
      val partitioners = read[BuiltinPartitioners](responseBody)
      assert(partitioners.partitioners.length > 0, "invalid response")
    }
  }
}
