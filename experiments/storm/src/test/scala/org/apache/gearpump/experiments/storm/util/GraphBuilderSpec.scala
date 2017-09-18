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

package org.apache.gearpump.experiments.storm.util

import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import org.apache.gearpump.experiments.storm.partitioner.StormPartitioner
import org.apache.gearpump.experiments.storm.topology.GearpumpStormTopology
import org.apache.gearpump.streaming.Processor
import org.apache.gearpump.streaming.task.Task

class GraphBuilderSpec extends WordSpec with Matchers with MockitoSugar {

  "GraphBuilder" should {
    "build Graph from Storm topology" in {
      val topology = mock[GearpumpStormTopology]

      val sourceId = "source"
      val sourceProcessor = mock[Processor[Task]]
      val targetId = "target"
      val targetProcessor = mock[Processor[Task]]

      when(topology.getProcessors).thenReturn(
        Map(sourceId -> sourceProcessor, targetId -> targetProcessor))
      when(topology.getTargets(sourceId)).thenReturn(Map(targetId -> targetProcessor))
      when(topology.getTargets(targetId)).thenReturn(Map.empty[String, Processor[Task]])

      val graph = GraphBuilder.build(topology)

      graph.getEdges.size shouldBe 1
      val (from, edge, to) = graph.getEdges.head
      from shouldBe sourceProcessor
      edge shouldBe a[StormPartitioner]
      to shouldBe targetProcessor
    }
  }
}
