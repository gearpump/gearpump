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

package io.gearpump.experiments.storm.util


import io.gearpump.experiments.storm.partitioner.StormPartitioner
import io.gearpump.experiments.storm.topology.GearpumpStormTopology
import io.gearpump.partitioner.Partitioner
import io.gearpump.streaming.Processor
import io.gearpump.streaming.task.Task
import io.gearpump.util.Graph

object GraphBuilder {

  /**
   * build a Gearpump DAG from a Storm topology
   * @param topology a wrapper over Storm topology
   * @return a DAG
   */
  def build(topology: GearpumpStormTopology): Graph[Processor[_ <: Task], _ <: Partitioner] = {
    val processorGraph = Graph.empty[Processor[Task], Partitioner]

    topology.getProcessors.foreach { case (sourceId, sourceProcessor) =>
      topology.getTargets(sourceId).foreach { case (targetId, targetProcessor) =>
        processorGraph.addEdge(sourceProcessor, new StormPartitioner(targetId), targetProcessor)
      }
    }

    processorGraph
  }
}
