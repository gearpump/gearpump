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

import backtype.storm.generated.{ComponentCommon, ComponentObject}
import backtype.storm.utils.Utils
import org.apache.gearpump.experiments.storm.util.{GraphBuilder, TopologyUtil}
import org.apache.gearpump.streaming.{ProcessorId, DAG, ProcessorDescription}
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
      val processors = DAG(processorGraph).processors
      val processorToComponent = graphBuilder.getProcessorToComponent
      val componentToProcessor = graphBuilder.getComponentToProcessor
      processorGraph.vertices.size shouldBe 4
      processorGraph.edges.length shouldBe 3
      processorToComponent.size shouldBe 4
      processorToComponent foreach { case (processor, component) =>
        if (spouts.containsKey(component)) {
          val spout = spouts.get(component)
          spout.get_spout_object().getSetField shouldBe ComponentObject._Fields.SERIALIZED_JAVA
          verifyParallelism(spout.get_common(), getTaskDescription(component, processors, componentToProcessor))
        } else if (bolts.containsKey(component)) {
          val bolt = bolts.get(component)
          bolt.get_bolt_object().getSetField shouldBe ComponentObject._Fields.SERIALIZED_JAVA
          verifyParallelism(bolt.get_common(), getTaskDescription(component, processors, componentToProcessor))
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

  private def getTaskDescription(component: String, processors: Map[ProcessorId, ProcessorDescription], componentToProcessor: Map[String, Int]): ProcessorDescription = {
    componentToProcessor.get(component).flatMap { processorId =>
      processors.get(processorId)
    }.getOrElse(fail(s"processor not found for component $component"))
  }

  private def verifyParallelism(componentCommon: ComponentCommon, taskDescription: ProcessorDescription): Unit = {
    if (componentCommon.get_parallelism_hint() == 0) {
      taskDescription.parallelism shouldBe 1
    } else {
      taskDescription.parallelism shouldBe componentCommon.get_parallelism_hint()
    }
  }

}
