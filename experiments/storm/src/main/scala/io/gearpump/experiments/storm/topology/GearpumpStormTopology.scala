/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.experiments.storm.topology

import java.util.{HashMap => JHashMap, Map => JMap}

import akka.actor.ActorSystem
import backtype.storm.Config
import backtype.storm.generated._
import backtype.storm.utils.ThriftTopologyUtils
import io.gearpump.cluster.UserConfig
import io.gearpump.experiments.storm.processor.StormProcessor
import io.gearpump.experiments.storm.producer.StormProducer
import io.gearpump.experiments.storm.util.StormConstants._
import io.gearpump.experiments.storm.util.StormUtil._
import io.gearpump.streaming.Processor
import io.gearpump.streaming.task.Task
import org.json.simple.JSONValue

import scala.collection.JavaConversions._

/**
 * @param topology storm topology
 * @param stormConfig storm configuration
 */
class GearpumpStormTopology(topology: StormTopology, stormConfig: JMap[AnyRef, AnyRef])(implicit system: ActorSystem) {

  stormConfig.put(Config.TOPOLOGY_KRYO_FACTORY, "io.gearpump.experiments.storm.util.StormKryoFactory")

  private val spouts = topology.get_spouts()
  private val bolts = topology.get_bolts()
  private val spoutProcessors = spouts.map { case (id, spout) =>
    id -> spoutToProcessor(id, spout) }.toMap
  private val boltProcessors = bolts.map { case (id, bolt) =>
    id -> boltToProcessor(id, bolt) }.toMap
  private val allProcessors = spoutProcessors ++ boltProcessors

  def getProcessors: Map[String, Processor[Task]] = allProcessors

  def getTargets(sourceId: String): Map[String, Processor[Task]] = {
    getTargets(sourceId, topology).map { case (targetId, _) =>
      targetId -> boltProcessors(targetId)
    }
  }

  private def spoutToProcessor(spoutId: String, spoutSpec: SpoutSpec)(implicit system: ActorSystem): Processor[Task] = {
    val componentCommon = spoutSpec.get_common()
    setComponentConfig(componentCommon)
    val taskConf = UserConfig.empty
        .withString(STORM_COMPONENT, spoutId)
    val parallelism = getParallelism(componentCommon)
    Processor[StormProducer](parallelism, spoutId, taskConf)

  }

  private def boltToProcessor(boltId: String, bolt: Bolt)(implicit system: ActorSystem): Processor[Task] = {
    val componentCommon = bolt.get_common()
    setComponentConfig(componentCommon)
    val taskConf = UserConfig.empty
        .withString(STORM_COMPONENT, boltId)
    val parallelism = getParallelism(componentCommon)
    Processor[StormProcessor](parallelism, boltId, taskConf)
  }

  /**
   * target components and streams
   */
  private def getTargets(componentId: String, topology: StormTopology): Map[String, Map[String, Grouping]] = {
    val componentIds = ThriftTopologyUtils.getComponentIds(topology)
    componentIds.flatMap { otherComponentId =>
      getInputs(otherComponentId, topology).toList.map(otherComponentId -> _)
    }.foldLeft(Map.empty[String, Map[String, Grouping]]) {
      (allTargets, componentAndInput) =>
        val (otherComponentId, (globalStreamId, grouping)) = componentAndInput
        val inputStreamId = globalStreamId.get_streamId()
        val inputComponentId = globalStreamId.get_componentId
        if (inputComponentId.equals(componentId)) {
          val curr = allTargets.getOrElse(otherComponentId, Map.empty[String, Grouping])
          allTargets + (otherComponentId -> (curr + (inputStreamId -> grouping)))
        } else {
          allTargets
        }
    }
  }

  private def getInputs(componentId: String, topology: StormTopology): JMap[GlobalStreamId, Grouping] = {
    ThriftTopologyUtils.getComponentCommon(topology, componentId).get_inputs
  }

  private def getParallelism(component: ComponentCommon): Int = {
    val componentConf = parseJsonStringToMap(component.get_json_conf)
    val parallelismHint = component.get_parallelism_hint()
    val numTasks = Option(componentConf.get(Config.TOPOLOGY_TASKS)).getOrElse(parallelismHint).asInstanceOf[Int]
    val parallelism = Option(componentConf.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM))
        .map(p => Math.min(p.asInstanceOf[Int], numTasks)).getOrElse(numTasks)

    if (parallelism == 0) {
      // for global grouping
      1
    } else {
      parallelism
    }
  }

  private def setComponentConfig(componentCommon: ComponentCommon): Unit = {
    Option(stormConfig).foreach { config =>
      val conf = new JHashMap[AnyRef, AnyRef]
      conf.putAll(config)
      val componentConfig = parseJsonStringToMap(componentCommon.get_json_conf())
      conf.putAll(componentConfig)
      componentCommon.set_json_conf(JSONValue.toJSONString(conf))
    }

  }


}
