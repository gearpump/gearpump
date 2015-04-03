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

import org.apache.gearpump.cluster.MasterToClient.SubmitApplicationResultValue
import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.streaming.ProcessorDescription
import org.apache.gearpump.streaming.appmaster.{StreamingAppMasterDataDetail, SubmitApplicationRequest}
import org.apache.gearpump.util.{Graph, LogUtil}
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.slf4j.Logger
import spray.http.{ContentTypes, HttpEntity}
import spray.testkit.ScalatestRouteTest
import upickle._

import scala.concurrent.duration._

class SubmitApplicationRequestServiceSpec extends FlatSpec with PropertyChecks with ScalatestRouteTest with SubmitApplicationRequestService with AppMasterService with Matchers {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  def actorRefFactory = system
  def master = TestCluster.master

  val dag = Graph.empty[Int,String]
  List(0,2,9,5,8,1,4,11,6,10,3,7).foreach(dag.addVertex)
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
  val submitApplicationRequest = SubmitApplicationRequest("complexdag",
    appJar = "examples/target/scala-2.11/gearpump-examples-complexdag-assembly-0.3.5-SNAPSHOT.jar",
    processors = Map(
      0 -> ProcessorDescription("org.apache.gearpump.streaming.examples.complexdag.Source_0", 1),
      5 -> ProcessorDescription("org.apache.gearpump.streaming.examples.complexdag.Sink_2", 1),
      10 -> ProcessorDescription("org.apache.gearpump.streaming.examples.complexdag.Node_3", 1),
      1 -> ProcessorDescription("org.apache.gearpump.streaming.examples.complexdag.Source_1", 1),
      6 -> ProcessorDescription("org.apache.gearpump.streaming.examples.complexdag.Sink_1", 1),
      9 -> ProcessorDescription("org.apache.gearpump.streaming.examples.complexdag.Node_4", 1),
      2 -> ProcessorDescription("org.apache.gearpump.streaming.examples.complexdag.Node_1", 1),
      7 -> ProcessorDescription("org.apache.gearpump.streaming.examples.complexdag.Node_0", 1),
      3 -> ProcessorDescription("org.apache.gearpump.streaming.examples.complexdag.Sink_0", 1),
      11 -> ProcessorDescription("org.apache.gearpump.streaming.examples.complexdag.Sink_3", 1),
      8 -> ProcessorDescription("org.apache.gearpump.streaming.examples.complexdag.Sink_4", 1),
      4 -> ProcessorDescription("org.apache.gearpump.streaming.examples.complexdag.Node_2", 1)
    ),
    dag = dag)
  var resultAppId = -1

  "SubmitApplicationRequestService" should "submit a complexdag and get back a valid appId" in {
    implicit val customTimeout = RouteTestTimeout(15.seconds)
    val jsonValue = write(submitApplicationRequest)
    Post(s"/api/$REST_VERSION/application", HttpEntity(ContentTypes.`application/json`, jsonValue)) ~> applicationRequestRoute ~> check {
      val responseBody = response.entity.asString
      val submitApplicationResult = read[SubmitApplicationResultValue](responseBody)
      validate(submitApplicationResult.appId >= 0, "invalid appid")
      resultAppId = submitApplicationResult.appId
    }
  }

  "SubmitApplicationRequestService" should "return an AppMasterDataDetail dag that matches SubmitApplicationRequest dag" in {
    Get(s"/api/$REST_VERSION/appmaster/${resultAppId}?detail=true") ~> appMasterRoute ~> check {
      val responseBody = response.entity.asString
      val appMasterDataDetail = read[StreamingAppMasterDataDetail](responseBody)
      def validateDag(dag: Graph[Int, Partitioner]): Boolean = {
        val vertices = dag.vertices.toIndexedSeq
        val mapping = submitApplicationRequest.dag.vertices.filterNot(vertex => {
          vertices.isDefinedAt(vertex)
        })
        mapping.size > 0
      }
      validateDag(appMasterDataDetail.dag)
    }
  }
}
