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

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.TestKit
import org.apache.gearpump.cluster.ClientToMaster.{ResolveAppId, SubmitApplication}
import org.apache.gearpump.cluster.MasterToAppMaster.AppMasterDataDetailRequest
import org.apache.gearpump.cluster.MasterToClient.{ResolveAppIdResult, SubmitApplicationResult, SubmitApplicationResultValue}
import org.apache.gearpump.partitioner.PartitionerDescription
import org.apache.gearpump.streaming.ProcessorDescription
import org.apache.gearpump.streaming.appmaster.{StreamingAppMasterDataDetail, SubmitApplicationRequest}
import org.apache.gearpump.util.{Constants, Graph, LogUtil}
import org.scalatest._
import org.slf4j.Logger
import spray.http.{ContentTypes, HttpEntity}
import spray.testkit.ScalatestRouteTest
import upickle._

import scala.util.Success

class SubmitApplicationRequestServiceSpec(sys:ActorSystem) extends FlatSpec
with ScalatestRouteTest with SubmitApplicationRequestService with AppMasterService with Matchers with BeforeAndAfterAll {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  def this() = this(ActorSystem("MySpec"))
  override implicit val system:ActorSystem = sys
  val _processors = Map(
    0 -> ProcessorDescription(0, "org.apache.gearpump.streaming.examples.complexdag.Source_0", 1),
    5 -> ProcessorDescription(5, "org.apache.gearpump.streaming.examples.complexdag.Sink_2", 1),
    10 -> ProcessorDescription(10, "org.apache.gearpump.streaming.examples.complexdag.Node_3", 1),
    1 -> ProcessorDescription(1, "org.apache.gearpump.streaming.examples.complexdag.Source_1", 1),
    6 -> ProcessorDescription(6, "org.apache.gearpump.streaming.examples.complexdag.Sink_1", 1),
    9 -> ProcessorDescription(9, "org.apache.gearpump.streaming.examples.complexdag.Node_4", 1),
    2 -> ProcessorDescription(2, "org.apache.gearpump.streaming.examples.complexdag.Node_1", 1),
    7 -> ProcessorDescription(7, "org.apache.gearpump.streaming.examples.complexdag.Node_0", 1),
    3 -> ProcessorDescription(3, "org.apache.gearpump.streaming.examples.complexdag.Sink_0", 1),
    11 -> ProcessorDescription(11, "org.apache.gearpump.streaming.examples.complexdag.Sink_3", 1),
    8 -> ProcessorDescription(8, "org.apache.gearpump.streaming.examples.complexdag.Sink_4", 1),
    4 -> ProcessorDescription(4, "org.apache.gearpump.streaming.examples.complexdag.Node_2", 1)
  )
  val dag = Graph.empty[Int, String]
  List(0, 2, 9, 5, 8, 1, 4, 11, 6, 10, 3, 7).foreach(dag.addVertex)
  dag.addEdge(0, "org.apache.gearpump.partitioner.HashPartitioner", 3)
  dag.addEdge(9, "org.apache.gearpump.partitioner.HashPartitioner", 11)
  dag.addEdge(2, "org.apache.gearpump.partitioner.HashPartitioner", 11)
  dag.addEdge(0, "org.apache.gearpump.partitioner.HashPartitioner", 6)
  dag.addEdge(2, "org.apache.gearpump.partitioner.HashPartitioner", 10)
  dag.addEdge(4, "org.apache.gearpump.partitioner.HashPartitioner", 10)
  dag.addEdge(7, "org.apache.gearpump.partitioner.HashPartitioner", 11)
  dag.addEdge(2, "org.apache.gearpump.partitioner.HashPartitioner", 9)
  dag.addEdge(10, "org.apache.gearpump.partitioner.HashPartitioner", 11)
  dag.addEdge(0, "org.apache.gearpump.partitioner.HashPartitioner", 10)
  dag.addEdge(0, "org.apache.gearpump.partitioner.HashPartitioner", 5)
  dag.addEdge(1, "org.apache.gearpump.partitioner.HashPartitioner", 8)
  dag.addEdge(0, "org.apache.gearpump.partitioner.HashPartitioner", 4)
  dag.addEdge(0, "org.apache.gearpump.partitioner.HashPartitioner", 2)
  dag.addEdge(1, "org.apache.gearpump.partitioner.HashPartitioner", 7)


  def actorRefFactory = system

  class MasterMock extends Actor {
    private val LOG: Logger = LogUtil.getLogger(getClass)
    def receive = {
      case ResolveAppId(appId) =>
        sender ! ResolveAppIdResult(Success(self))
      case SubmitApplication(appDescription, appJar, username) =>
        LOG.info("fake master SubmitApplication")
        sender ! SubmitApplicationResult(Success(1))
      case AppMasterDataDetailRequest(appId) =>
        LOG.info("fake master AppMasterDataDetailRequest")
        val jsonData =
          """
            |{"appId":1,"appName":"complexdag",
            |"actorPath":"akka.tcp://app1-executor-1@127.0.0.1:54963/user/daemon/appdaemon1/$c/appmaster",
            |"clock":1.430263340412E12,
            |"executors":[
            |[0,"akka.tcp://app1system0@127.0.0.1:54968/remote/akka.tcp/app1-executor-1@127.0.0.1:54963/user/daemon/appdaemon1/$c/appmaster/executors/0"],
            |[1,"akka.tcp://app1system1@127.0.0.1:54967/remote/akka.tcp/app1-executor-1@127.0.0.1:54963/user/daemon/appdaemon1/$c/appmaster/executors/1"]],
            |"tasks":[
            |[{"processorId":5,"index":0},1],
            |[{"processorId":9,"index":0},1],
            |[{"processorId":0,"index":0},1],
            |[{"processorId":4,"index":0},0],
            |[{"processorId":2,"index":0},0],
            |[{"processorId":3,"index":0},0],
            |[{"processorId":8,"index":0},0],
            |[{"processorId":10,"index":0},1],
            |[{"processorId":6,"index":0},1],
            |[{"processorId":11,"index":0},0],
            |[{"processorId":1,"index":0},1],
            |[{"processorId":7,"index":0},0]],
            |"processors":[
            |[0,{"id":0, "taskClass":"org.apache.gearpump.streaming.examples.complexdag.Source_0","parallelism":1}],
            |[5,{"id":5, "taskClass":"org.apache.gearpump.streaming.examples.complexdag.Sink_1","parallelism":1}],
            |[10,{"id":10, "taskClass":"org.apache.gearpump.streaming.examples.complexdag.Node_3","parallelism":1}],
            |[1,{"id":1, "taskClass":"org.apache.gearpump.streaming.examples.complexdag.Source_1","parallelism":1}],
            |[6,{"id":6, "taskClass":"org.apache.gearpump.streaming.examples.complexdag.Sink_2","parallelism":1}],
            |[9,{"id":9, "taskClass":"org.apache.gearpump.streaming.examples.complexdag.Node_4","parallelism":1}],
            |[2,{"id":2, "taskClass":"org.apache.gearpump.streaming.examples.complexdag.Node_1","parallelism":1}],
            |[7,{"id":7, "taskClass":"org.apache.gearpump.streaming.examples.complexdag.Sink_4","parallelism":1}],
            |[3,{"id":3, "taskClass":"org.apache.gearpump.streaming.examples.complexdag.Node_2","parallelism":1}],
            |[11,{"id":11, "taskClass":"org.apache.gearpump.streaming.examples.complexdag.Sink_3","parallelism":1}],
            |[8,{"id":8, "taskClass":"org.apache.gearpump.streaming.examples.complexdag.Node_0","parallelism":1}],
            |[4,{"id":4,"taskClass":"org.apache.gearpump.streaming.examples.complexdag.Sink_0","parallelism":1}]],
            |"processorLevels":[[0,0],[5,1],[10,2],[1,0],[6,1],[9,2],[2,1],[7,1],[3,1],[11,3],[8,1],[4,1]],
            |"dag":{"vertices":[0,2,9,5,8,1,4,11,6,10,3,7],
            |"edges":[
            |[10,"org.apache.gearpump.partitioner.HashPartitioner",11],
            |[0,"org.apache.gearpump.partitioner.HashPartitioner",6],
            |[1,"org.apache.gearpump.partitioner.HashPartitioner",8],
            |[2,"org.apache.gearpump.partitioner.HashPartitioner",10],
            |[0,"org.apache.gearpump.partitioner.HashPartitioner",10],
            |[0,"org.apache.gearpump.partitioner.HashPartitioner",4],
            |[8,"org.apache.gearpump.partitioner.HashPartitioner",11],
            |[0,"org.apache.gearpump.partitioner.HashPartitioner",3],
            |[1,"org.apache.gearpump.partitioner.HashPartitioner",7],
            |[3,"org.apache.gearpump.partitioner.HashPartitioner",10],
            |[0,"org.apache.gearpump.partitioner.HashPartitioner",2],
            |[0,"org.apache.gearpump.partitioner.HashPartitioner",5],
            |[9,"org.apache.gearpump.partitioner.HashPartitioner",11],
            |[2,"org.apache.gearpump.partitioner.HashPartitioner",9],
            |[2,"org.apache.gearpump.partitioner.HashPartitioner",11]]}}
          """.stripMargin
        val appMasterDataDetail = read[StreamingAppMasterDataDetail](jsonData)
        sender ! appMasterDataDetail
      case unknown =>
        LOG.info(s"unknown message ${unknown.getClass.getName}!!!")
    }
  }

  def master = system.actorOf(Props(new MasterMock))

  val submitApplicationRequest = SubmitApplicationRequest("complexdag",
    appJar = null,
    processors = _processors,
    dag = dag)
  var resultAppId = -1

  "SubmitApplicationRequestServiceSpec" should "submit a SubmitApplicationRequest and get an appId > 0" in {
    implicit val timeout = Constants.FUTURE_TIMEOUT
      val jsonValue = write(submitApplicationRequest)
      Post(s"/api/$REST_VERSION/application", HttpEntity(ContentTypes.`application/json`, jsonValue)) ~> applicationRequestRoute ~> check {
        val responseBody = response.entity.asString
        val submitApplicationResultValue = read[SubmitApplicationResultValue](responseBody)
        validate(submitApplicationResultValue.appId >= 0, "invalid appid")
        resultAppId = submitApplicationResultValue.appId
        LOG.info(s"submitApplicationResult.appId=${submitApplicationResultValue.appId} setting resultAppId=${resultAppId}")
      }
    }

  "SubmitApplicationRequestServiceSpec" should "return an AppMasterDataDetail dag that matches SubmitApplicationRequest dag" in {
    LOG.info(s"resultAppId=${resultAppId}")
    Get(s"/api/$REST_VERSION/appmaster/$resultAppId?detail=true") ~> appMasterRoute ~> check {
      val responseBody = response.entity.asString
      val appMasterDataDetail = read[StreamingAppMasterDataDetail](responseBody)
      def validateDag(dag: Graph[Int, PartitionerDescription]): Unit = {
        val vertices = dag.vertices.toIndexedSeq
        val mapping = submitApplicationRequest.dag.vertices.filterNot(vertex => {
          vertices.isDefinedAt(vertex)
        })
        assert(mapping.size == 0)
        assert(dag.edges.size == submitApplicationRequest.dag.edges.size)
        /*
        val edges = dag.vertices.foreach(vertex => {
          val edges1 = dag.edgesOf(vertex)
          val edges2 = submitApplicationRequest.dag.edgesOf(vertex)
          assert(edges1.size == edges2.size)
        })
        */
      }
      validateDag(appMasterDataDetail.dag)
    }
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }
}
