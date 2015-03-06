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

package org.apache.gearpump.experiments.storm.util

import backtype.storm.generated.{Grouping, StormTopology}
import backtype.storm.tuple.Fields
import backtype.storm.utils.ThriftTopologyUtils
import org.apache.gearpump.experiments.storm.partitioner.{NoneGroupingPartitioner, ShuffleGroupingPartitioner, FieldsGroupingPartitioner, GlobalGroupingPartitioner}
import org.apache.gearpump.experiments.storm.processor.StormProcessor
import org.apache.gearpump.experiments.storm.producer.StormProducer
import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.streaming.TaskDescription
import org.apache.gearpump.util.Graph

import scala.collection.JavaConversions._

object GraphBuilder {
  def apply(topology: StormTopology): GraphBuilder = new GraphBuilder(topology)
}

private[storm] class GraphBuilder(topology: StormTopology) {
  private val componentGraph = Graph.empty[String, Grouping]
  private val processorGraph = Graph.empty[TaskDescription, Partitioner]
  private var processorToComponent = Map.empty[Int, String]
  private var componentToProcessor = Map.empty[String, Int]

  def build(): Unit = {
    val spouts = topology.get_spouts()
    val spoutTasks = spouts.map { spout =>
      val parallelism = spout._2.get_common().get_parallelism_hint()
      val taskDescription = TaskDescription(classOf[StormProducer].getName, parallelism)
      spout._1 -> taskDescription
    }
    val bolts = topology.get_bolts()
    val boltTasks = bolts.map { bolt =>
      val parallelism = bolt._2.get_common().get_parallelism_hint()
      val taskDescription = TaskDescription(classOf[StormProcessor].getName, parallelism)
      bolt._1 -> taskDescription
    }

    spoutTasks.foreach { case (sourceId, sourceProducer) =>
      val sourceSpec = spouts.get(sourceId)
      getTargets(sourceId).foreach { case (streamId, targets) =>
        val outFields = new Fields(sourceSpec.get_common().get_streams().get(streamId).get_output_fields())
        targets.foreach { case (targetId, grouping) =>
          boltTasks.get(targetId).foreach { targetProcessor =>
            componentGraph.addEdge(sourceId, grouping, targetId)
            processorGraph.addEdge(sourceProducer, groupingToPartitioner(outFields, grouping), targetProcessor)
          }
        }
      }
    }

    boltTasks.foreach { case (sourceId, sourceProcessor) =>
      val sourceSpec = bolts.get(sourceId)
      getTargets(sourceId).foreach { case (streamId, targets) =>
        val outFields = new Fields(sourceSpec.get_common().get_streams().get(streamId).get_output_fields())
        targets.foreach { case (targetId, grouping) =>
          boltTasks.get(targetId).foreach { targetProcessor =>
            componentGraph.addEdge(sourceId, grouping, targetId)
            processorGraph.addEdge(sourceProcessor, groupingToPartitioner(outFields, grouping), targetProcessor)
          }
        }
      }
    }

    val topologyWithIndex = componentGraph.topologicalOrderIterator.zipWithIndex
    componentToProcessor = topologyWithIndex.toMap
    processorToComponent = componentToProcessor.map(entry => entry._2 -> entry._1)
  }

  def getProcessorGraph: Graph[TaskDescription, Partitioner] = processorGraph

  def getProcessorToComponent: Map[Int, String] = processorToComponent

  def getComponenToProcessor: Map[String, Int] = componentToProcessor

  def getTargets(componentId: String): Map[String, Map[String, Grouping]] = {
    val componentIds = ThriftTopologyUtils.getComponentIds(topology)
    componentIds.flatMap { otherComponentId =>
      ThriftTopologyUtils.getComponentCommon(topology, otherComponentId).get_inputs.toList.map(otherComponentId -> _)
    }.foldLeft(Map.empty[String, Map[String, Grouping]]) {
      (allTargets, componentAndInput) =>
        val (otherComponentId, (globalStreamId, grouping)) = componentAndInput
        val inputStreamId = globalStreamId.get_streamId()
        val inputComponentId = globalStreamId.get_componentId
        if (inputComponentId.equals(componentId)) {
          val curr = allTargets.getOrElse(inputStreamId, Map.empty[String, Grouping])
          allTargets + (inputStreamId -> (curr + (otherComponentId -> grouping)))
        } else {
          allTargets
        }
    }
  }

  def groupingToPartitioner(outFields: Fields, grouping: Grouping): Partitioner = {
    grouping.getSetField match {
      case Grouping._Fields.FIELDS =>
        if (isGlobalGrouping(grouping)) new GlobalGroupingPartitioner
        else new FieldsGroupingPartitioner(outFields, new Fields(grouping.get_fields()))
      case Grouping._Fields.SHUFFLE =>
        new ShuffleGroupingPartitioner
      case Grouping._Fields.ALL =>
        throw new RuntimeException("all grouping not supported")
      case Grouping._Fields.NONE =>
        new NoneGroupingPartitioner
      case Grouping._Fields.CUSTOM_SERIALIZED =>
        throw new RuntimeException("custom serialized grouping not supported")
      case Grouping._Fields.CUSTOM_OBJECT =>
        throw new RuntimeException("custom object grouping not supported")
      case Grouping._Fields.DIRECT =>
        throw new RuntimeException("direct grouping not supported")
      case Grouping._Fields.LOCAL_OR_SHUFFLE =>
        // GearPump has built-in support for sending messages to local actor
        new ShuffleGroupingPartitioner
    }
  }

  def isGlobalGrouping(grouping: Grouping): Boolean = {
    grouping.getSetField == Grouping._Fields.FIELDS &&
    grouping.get_fields.isEmpty
  }
}
