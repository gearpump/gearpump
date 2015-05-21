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

import akka.actor.ActorSystem
import backtype.storm.generated.{Bolt, SpoutSpec, ComponentCommon}
import backtype.storm.utils.Utils
import org.apache.gearpump.cluster.TestUtil
import org.apache.gearpump.experiments.storm.util.{TopologyUtil, GraphBuilder}
import org.apache.gearpump.experiments.storm.util.GraphBuilder._
import org.apache.gearpump.streaming.{Processor, ProcessorId, DAG, ProcessorDescription}
import org.scalatest.{Matchers, WordSpec}

class GraphBuilderSpec extends WordSpec with Matchers {

  "GraphBuilder" should {
    val topology = TopologyUtil.getTestTopology
    val spouts = topology.get_spouts()
    val bolts = topology.get_bolts()

    "build Graph from Storm topology" in {
      implicit val system = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
      val graphBuilder = new GraphBuilder()
      val processorGraph = graphBuilder.build(topology)
      processorGraph.vertices.size shouldBe 4
      processorGraph.edges.length shouldBe 3

      var processorIdIndex = 0
      val processorDescriptionGraph = processorGraph.mapVertex { processor =>
        val description = Processor.ProcessorToProcessorDescription(processorIdIndex, processor)
        processorIdIndex += 1
        description
      }
      val dag: DAG = processorDescriptionGraph
      val processors = dag.processors

      processors foreach { case (pid, procDesc) =>
        val conf = procDesc.taskConf
        val cid = conf.getString(COMPONENT_ID).getOrElse(
          fail(s"component id not found for processor $pid"))

        if (spouts.containsKey(cid)) {
          val spoutSpec = conf.getValue[SpoutSpec](COMPONENT_SPEC).getOrElse(
            fail(s"spout spec not found for processor $pid")
          )
          spoutSpec shouldBe spouts.get(cid)
          verifyParallelism(spoutSpec.get_common(), procDesc)
        } else if (bolts.containsKey(cid)) {
          val bolt = conf.getValue[Bolt](COMPONENT_SPEC).getOrElse(
            fail(s"bolt not found for processor $pid")
          )
          bolt shouldBe bolts.get(cid)
          verifyParallelism(bolt.get_common(), procDesc)
        }
      }
    }

    "get target components for a source of topology" in {
      val graphBuilder = new GraphBuilder
      val targets = graphBuilder.getTargets("2", topology)
      targets.contains(Utils.DEFAULT_STREAM_ID)  shouldBe true
      targets.get(Utils.DEFAULT_STREAM_ID).get.contains("3") shouldBe true
      targets.get(Utils.DEFAULT_STREAM_ID).get("3").is_set_fields shouldBe true
    }
  }

  private def verifyParallelism(componentCommon: ComponentCommon, taskDescription: ProcessorDescription): Unit = {
    if (componentCommon.get_parallelism_hint() == 0) {
      taskDescription.parallelism shouldBe 1
    } else {
      taskDescription.parallelism shouldBe componentCommon.get_parallelism_hint()
    }
  }

}
