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

import akka.actor.ActorSystem
import backtype.storm.generated.{ComponentCommon, Grouping, StormTopology}
import backtype.storm.tuple.Fields
import backtype.storm.utils.ThriftTopologyUtils
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.experiments.storm.partitioner.{NoneGroupingPartitioner, FieldsGroupingPartitioner, GlobalGroupingPartitioner}
import org.apache.gearpump.experiments.storm.processor.StormProcessor
import org.apache.gearpump.experiments.storm.producer.StormProducer
import org.apache.gearpump.partitioner.{ShuffleGroupingPartitioner, Partitioner}
import org.apache.gearpump.streaming.task.Task
import org.apache.gearpump.streaming.{Processor, ProcessorDescription}
import org.apache.gearpump.util.Graph

import scala.collection.JavaConversions._

object GraphBuilder {
  val COMPONENT_ID = "component_id"
  val COMPONENT_SPEC = "component_spec"
}

private[storm] class GraphBuilder {
  import org.apache.gearpump.experiments.storm.util.GraphBuilder._

  def build(topology: StormTopology)(implicit system: ActorSystem): Graph[Processor[_ <: Task], Partitioner] = {
    val processorGraph = Graph.empty[Processor[_ <: Task], Partitioner]

    val spouts = topology.get_spouts()
    val spoutTasks = spouts.map { case (id, spec) =>
      val parallelism = getParallelism(spec.get_common())
      val processor = Processor[StormProducer](parallelism,
        taskConf = UserConfig.empty
          .withString(COMPONENT_ID, id)
          .withValue(COMPONENT_SPEC, spec)
      )
      id -> processor
    }
    val bolts = topology.get_bolts()
    val boltTasks = bolts.map { case (id, spec) =>
      val parallelism = getParallelism(spec.get_common())
      val processor = Processor[StormProcessor](parallelism,
        taskConf = UserConfig.empty
          .withString(COMPONENT_ID, id)
          .withValue(COMPONENT_SPEC, spec)
      )
      id -> processor
    }

    spoutTasks.foreach { case (sourceId, sourceProducer) =>
      val sourceSpec = spouts.get(sourceId)
      getTargets(sourceId, topology).foreach { case (streamId, targets) =>
        val outFields = new Fields(sourceSpec.get_common().get_streams().get(streamId).get_output_fields())
        targets.foreach { case (targetId, grouping) =>
          boltTasks.get(targetId).foreach { targetProcessor =>
            processorGraph.addEdge(sourceProducer, groupingToPartitioner(outFields, grouping), targetProcessor)
          }
        }
      }
    }

    boltTasks.foreach { case (sourceId, sourceProcessor) =>
      val sourceSpec = bolts.get(sourceId)
      getTargets(sourceId, topology).foreach { case (streamId, targets) =>
        val outFields = new Fields(sourceSpec.get_common().get_streams().get(streamId).get_output_fields())
        targets.foreach { case (targetId, grouping) =>
          boltTasks.get(targetId).foreach { targetProcessor =>
            processorGraph.addEdge(sourceProcessor, groupingToPartitioner(outFields, grouping), targetProcessor)
          }
        }
      }
    }

    processorGraph
  }

  def getTargets(componentId: String, topology: StormTopology): Map[String, Map[String, Grouping]] = {
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

  def getParallelism(component: ComponentCommon): Int = {
    val parallelism = component.get_parallelism_hint()
    if (parallelism == 0) {
      // for global grouping
      1
    } else {
      parallelism
    }
  }
}
