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

package org.apache.gearpump.services.util

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.metrics.Metrics.{Counter, MetricType}
import org.apache.gearpump.streaming.appmaster.DagManager.ReplaceProcessor
import org.apache.gearpump.streaming.{ProcessorDescription, ProcessorId}
import org.apache.gearpump.streaming.appmaster.StreamingAppMasterDataDetail
import org.apache.gearpump.util.{Graph}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import upickle.default.{read, write}
import UpickleUtil._
import upickle.default.{read, write}

class UpickleSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  "UserConfig" should "serialize and deserialize with upickle correctly" in {
    val conf = UserConfig.empty.withString("key", "value")
    val serialized = write(conf)
    val deserialized = read[UserConfig](serialized)
    assert(deserialized.getString("key") == Some("value"))
  }

  "Graph" should "be able to serialize/deserialize correctly" in {
    val graph = new Graph[Int, String](List(0, 1), List((0, "edge", 1)))
    val serialized = write(graph)

    val deserialized = read[Graph[Int, String]](serialized)

    graph.vertices.toSet shouldBe deserialized.vertices.toSet
    graph.edges.toSet shouldBe deserialized.edges.toSet
  }

  "MetricType" should "be able to serialize/deserialize correctly" in {
    val metric: MetricType = Counter("counter", 100L)
    val serialized = write(metric)
    val deserialized = read[MetricType](serialized)
    metric shouldBe deserialized
  }

  "StreamingAppMasterDataDetail" should "serialize and deserialize with upickle correctly" in {
    val app = new StreamingAppMasterDataDetail(appId = 0,
      processors = Map.empty[ProcessorId, ProcessorDescription],
      processorLevels = Map.empty[ProcessorId, Int]
    )

    val serialized = write(app)
    val deserialized = read[StreamingAppMasterDataDetail](serialized)
    assert(deserialized == app)
  }
}