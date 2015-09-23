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

package io.gearpump.experiments.storm.util

import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import akka.actor.ActorSystem
import backtype.storm.generated._
import backtype.storm.spout.{ISpout, ShellSpout}
import backtype.storm.task.{IBolt, ShellBolt}
import backtype.storm.tuple.Fields
import backtype.storm.utils.Utils
import io.gearpump.cluster.UserConfig
import io.gearpump.experiments.storm.processor.StormProcessor
import io.gearpump.experiments.storm.producer.StormProducer
import io.gearpump.partitioner.Partitioner
import io.gearpump.streaming.Processor
import io.gearpump.streaming.task.Task
import io.gearpump.util.Graph

import scala.collection.JavaConversions._

object GraphBuilder {
  val COMPONENT_ID = "component_id"
  val COMPONENT_SPEC = "component_spec"

  import StormUtil._

  def build(topology: StormTopology)(implicit system: ActorSystem): (Graph[Processor[_ <: Task], _ <: Partitioner], Map[Integer, String]) = {
    val processorGraph = Graph.empty[Processor[Task], Partitioner]
    var taskToComponent = Map.empty[Integer, String]

    val spouts = topology.get_spouts()
    val bolts = topology.get_bolts()
    var taskId = 0
    var multiLang = false

    val spoutProcessors = spouts.map { case (id, spout) => id -> spoutToProcessor(id, spout) }.toMap
    val boltProcessors = bolts.map { case (id, bolt) => id -> boltToProcessor(id, bolt) }.toMap

    spouts.foreach { case (sourceId, spout) =>
      val spoutObject = Utils.getSetComponentObject(spout.get_spout_object()).asInstanceOf[ISpout]
      if (spoutObject.isInstanceOf[ShellSpout]) {
        multiLang = true
      }

      val sourceProcessor = spoutProcessors(sourceId)
      val sourceCommon = spout.get_common()
      val sourceParallelism = getParallelism(sourceCommon)
      val tasks = getTasks(sourceId, taskId, sourceParallelism)
      taskId += sourceParallelism
      tasks.foreach { t =>
        taskToComponent += t -> sourceId
      }
      addEdge(processorGraph, topology, bolts, boltProcessors, sourceId, sourceCommon, sourceProcessor)
    }

    bolts.foreach { case (sourceId, bolt) =>
      val boltObject = Utils.getSetComponentObject(bolt.get_bolt_object()).asInstanceOf[IBolt]
      if (boltObject.isInstanceOf[ShellBolt]) {
        multiLang = true
      }
      val sourceProcessor = boltProcessors(sourceId)
      val sourceCommon = bolt.get_common()
      val sourceParallelism = getParallelism(sourceCommon)
      val tasks = getTasks(sourceId, taskId, sourceParallelism)
      taskId += sourceParallelism
      tasks.foreach { t =>
        taskToComponent += t -> sourceId
      }
      addEdge(processorGraph, topology, bolts, boltProcessors, sourceId, sourceCommon, sourceProcessor)
    }
    (processorGraph, taskToComponent)
  }

  def getTasks(sourceId: String, startId: Integer, num: Int): JList[Integer] = {
    val tasks = new JArrayList[Integer](num)
    (0 until num).foreach(i => tasks.add(startId + i))
    tasks
  }

  def spoutToProcessor(id: String, spout: SpoutSpec)(implicit system: ActorSystem): Processor[Task] = {
    val parallelism = getParallelism(spout.get_common())
    Processor[StormProducer](parallelism,
      description = id,
      taskConf = UserConfig.empty
          .withString(COMPONENT_ID, id)
          .withValue(COMPONENT_SPEC, spout)
    )
  }

  def boltToProcessor(id: String, bolt: Bolt)(implicit system: ActorSystem): Processor[Task] = {
    val parallelism = getParallelism(bolt.get_common())
    Processor[StormProcessor](parallelism,
      description = id,
      taskConf = UserConfig.empty
          .withString(COMPONENT_ID, id)
          .withValue(COMPONENT_SPEC, bolt)
    )
  }

  def addEdge(processorGraph: Graph[Processor[Task], Partitioner], topology: StormTopology,
      bolts: JMap[String, Bolt], boltProcessors: Map[String, Processor[Task]],
      sourceId: String, sourceCommon: ComponentCommon, sourceProcessor: Processor[Task])(implicit system: ActorSystem): Unit = {
    StormUtil.getTargets(sourceId, topology).foreach { case (streamId, targets) =>
      val outFields = new Fields(sourceCommon.get_streams().get(streamId).get_output_fields())
      targets.foreach { case (targetId, grouping) =>
        if (grouping.getSetField != Grouping._Fields.DIRECT) {
          val targetProcessor = boltProcessors(targetId)
          processorGraph.addEdge(
            sourceProcessor,
            groupingToPartitioner(outFields, grouping, targetId),
            targetProcessor)
        }
      }
    }
  }
}
