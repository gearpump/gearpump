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

package org.apache.gearpump.experiments.storm

import backtype.storm.generated.ComponentObject
import backtype.storm.utils.Utils
import org.apache.gearpump.experiments.storm.util.{GraphBuilder, TopologyUtil}
import org.scalatest.{Matchers, WordSpec}

class GraphBuilderSpec extends WordSpec with Matchers {

  "GraphBuilder" should {
    val topology = TopologyUtil.getTestTopology
    val spouts = topology.get_spouts()
    val bolts = topology.get_bolts()

    "build Graph from Storm topology" in {
      val graphBuilder = new GraphBuilder(topology)
      graphBuilder.build()
      val processorGraph = graphBuilder.getProcessorGraph
      val processorToComponent = graphBuilder.getProcessorToComponent
      processorGraph.vertices.size shouldBe 4
      processorGraph.edges.length shouldBe 3
      processorToComponent.size shouldBe 4
      processorToComponent foreach { case (processor, component) =>
        if (spouts.containsKey(component)) {
          spouts.get(component).get_spout_object().getSetField shouldBe ComponentObject._Fields.SERIALIZED_JAVA
        } else if (bolts.containsKey(component)) {
          bolts.get(component).get_bolt_object().getSetField shouldBe ComponentObject._Fields.SERIALIZED_JAVA
        }
      }
    }

    "get target components for a source of topology" in {
      val graphBuilder = new GraphBuilder(topology)
      val targets = graphBuilder.getTargets("2")
      targets.contains(Utils.DEFAULT_STREAM_ID)  shouldBe true
      targets.get(Utils.DEFAULT_STREAM_ID).get.contains("3") shouldBe true
      targets.get(Utils.DEFAULT_STREAM_ID).get("3").is_set_fields shouldBe true
    }
  }

}
