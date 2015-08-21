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

package io.gearpump.experiments.storm

import akka.actor.ActorSystem
import io.gearpump.cluster.TestUtil
import io.gearpump.experiments.storm.topology.GearpumpStormTopology
import io.gearpump.experiments.storm.util.{GraphBuilder, StormConstants, TopologyUtil}
import io.gearpump.partitioner.{PartitionerDescription, PartitionerObject}
import io.gearpump.streaming.{DAG, Processor}
import org.scalatest.{Matchers, WordSpec}

class GraphBuilderSpec extends WordSpec with Matchers {
  import StormConstants._

  "GraphBuilder" should {
    val topology = TopologyUtil.getTestTopology
    val spouts = topology.get_spouts()
    val bolts = topology.get_bolts()

    "build Graph from Storm topology" in {
      implicit val system = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
      val gearpumpStormTopology = new GearpumpStormTopology(topology, null)
      val processorGraph = GraphBuilder.build(gearpumpStormTopology)
      processorGraph.vertices.size shouldBe 4
      processorGraph.edges.length shouldBe 3

      var processorIdIndex = 0
      val processorDescriptionGraph = processorGraph.mapVertex { processor =>
        val description = Processor.ProcessorToProcessorDescription(processorIdIndex, processor)
        processorIdIndex += 1
        description
      }.mapEdge { (node1, edge, node2) =>
        PartitionerDescription(new PartitionerObject(edge))
      }

      val dag: DAG = DAG(processorDescriptionGraph)
      val processors = dag.processors

      processors foreach { case (pid, procDesc) =>
        val conf = procDesc.taskConf
        conf.getString(STORM_COMPONENT).getOrElse(
          fail(s"component id not found for processor $pid"))
      }
    }
  }
}
