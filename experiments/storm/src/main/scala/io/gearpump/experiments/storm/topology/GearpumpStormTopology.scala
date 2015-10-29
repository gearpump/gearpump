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

import java.io._
import java.lang.{Iterable => JIterable}
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}

import akka.actor.ActorSystem
import backtype.storm.Config
import backtype.storm.generated._
import backtype.storm.utils.{ThriftTopologyUtils, Utils}
import io.gearpump.cluster.UserConfig
import io.gearpump.experiments.storm.processor.StormProcessor
import io.gearpump.experiments.storm.producer.StormProducer
import io.gearpump.experiments.storm.util.StormConstants._
import io.gearpump.experiments.storm.util.StormUtil._
import io.gearpump.streaming.Processor
import io.gearpump.streaming.task.Task
import io.gearpump.util.LogUtil
import org.slf4j.Logger
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.SafeConstructor

import scala.collection.JavaConversions._

/**
 * @param topology storm topology
 * @param appConfig json config submitted from user application
 * @param fileConfig custom file config set by user in command line
 */
class GearpumpStormTopology(topology: StormTopology, appConfig: String, fileConfig: String)(implicit system: ActorSystem) {
  import io.gearpump.experiments.storm.topology.GearpumpStormTopology._

  private val spouts = topology.get_spouts()
  private val bolts = topology.get_bolts()
  private val spoutProcessors = spouts.map { case (id, spout) =>
    id -> spoutToProcessor(id, spout) }.toMap
  private val boltProcessors = bolts.map { case (id, bolt) =>
    id -> boltToProcessor(id, bolt) }.toMap
  private val allProcessors = spoutProcessors ++ boltProcessors


  private val stormConfig = mergeConfigs(appConfig, fileConfig, getComponentConfigs(spouts, bolts))
  println(stormConfig)

  /**
   * merge configs from application, custom config file and component
   * with priority
   *    defaults.yaml < storm.yaml < application config < component config < custom file config
   */
  private def mergeConfigs(appConfig: String, fileConfig: String,
                           componentConfigs: Iterable[JMap[AnyRef, AnyRef]]): JMap[AnyRef, AnyRef] = {
    val allConfig = Utils.readStormConfig().asInstanceOf[JMap[AnyRef, AnyRef]]
    allConfig.putAll(parseJsonStringToMap(appConfig))
    allConfig.putAll(getMergedComponentConfig(componentConfigs, allConfig))
    allConfig.putAll(readStormConfig(fileConfig))
    allConfig
  }

  def unique(list: List[AnyRef]): AnyRef = {
    list.distinct
  }

  def getStormConfig: JMap[AnyRef, AnyRef] = stormConfig

  def getProcessors: Map[String, Processor[Task]] = allProcessors

  def getTargets(sourceId: String): Map[String, Processor[Task]] = {
    getTargets(sourceId, topology).map { case (targetId, _) =>
      targetId -> boltProcessors(targetId)
    }
  }

  private def spoutToProcessor(spoutId: String, spoutSpec: SpoutSpec)(implicit system: ActorSystem): Processor[Task] = {
    val componentCommon = spoutSpec.get_common()
    val taskConf = UserConfig.empty
        .withString(STORM_COMPONENT, spoutId)
    val parallelism = getParallelism(stormConfig, componentCommon)
    Processor[StormProducer](parallelism, spoutId, taskConf)

  }

  private def boltToProcessor(boltId: String, bolt: Bolt)(implicit system: ActorSystem): Processor[Task] = {
    val componentCommon = bolt.get_common()
    val taskConf = UserConfig.empty
        .withString(STORM_COMPONENT, boltId)
    val parallelism = getParallelism(stormConfig, componentCommon)
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

  private def getParallelism(allConfig: JMap[AnyRef, AnyRef], component: ComponentCommon): Int = {
    val parallelismHint: Int = if (component.is_set_parallelism_hint()) {
      component.get_parallelism_hint()
    } else {
      1
    }
    val mergedConfig = new JHashMap[AnyRef, AnyRef]
    val componentConfig = parseJsonStringToMap(component.get_json_conf)
    mergedConfig.putAll(allConfig)
    mergedConfig.putAll(componentConfig)
    val numTasks: Int = getInt(mergedConfig, Config.TOPOLOGY_TASKS).getOrElse(parallelismHint)
    val parallelism: Int = getInt(mergedConfig, Config.TOPOLOGY_MAX_TASK_PARALLELISM)
      .fold(numTasks)(p => math.min(p, numTasks))
    parallelism
  }

  private def getComponentConfigs(spouts: JMap[String, SpoutSpec],
                                  bolts: JMap[String, Bolt]): Iterable[JMap[AnyRef, AnyRef]] = {
    spouts.map { case (id, spoutSpec) =>
      parseJsonStringToMap(spoutSpec.get_common().get_json_conf())
    } ++ bolts.map { case (id, boltSpec) =>
      parseJsonStringToMap(boltSpec.get_common().get_json_conf())
    }
  }

  private def getMergedComponentConfig(componentConfigs: Iterable[JMap[AnyRef, AnyRef]],
                                       allConfig: JMap[AnyRef, AnyRef]): JMap[AnyRef, AnyRef] = {
    val mergedConfig: JMap[AnyRef, AnyRef] = new JHashMap[AnyRef, AnyRef]
    mergeKryoDecorators(componentConfigs, allConfig, mergedConfig)
    mergeKryoRegister(componentConfigs, allConfig, mergedConfig)
    mergedConfig
  }

  private def getConfigValues(componentConfigs: Iterable[JMap[AnyRef, AnyRef]],
                              allConfig: JMap[AnyRef, AnyRef], key: String): Iterable[AnyRef] = {
    componentConfigs.map(config => config.get(key)) ++ Option(allConfig.get(key)).toList
  }

  private def mergeKryoDecorators(componentConfigs: Iterable[JMap[AnyRef, AnyRef]],
                                  allConfig: JMap[AnyRef, AnyRef],
                                  mergedConfig: JMap[AnyRef, AnyRef]): Unit = {
    val key = Config.TOPOLOGY_KRYO_DECORATORS
    val configs = getConfigValues(componentConfigs, allConfig, key)
    val distincts = configs.foldLeft(Set.empty[String]) {
      case (accum, config: JIterable[_]) =>
        accum ++ config.map {
          case s: String => s
          case illegal =>
            throw new IllegalArgumentException(s"$key must be a List of Strings; actually $illegal")
        }
      case (accum, null) =>
        accum
      case illegal =>
        throw new IllegalArgumentException(s"$key must be a List of Strings; actually $illegal")

    }
    if (distincts.nonEmpty) {
      val decorators: JList[String] = new JArrayList(distincts.size)
      decorators.addAll(distincts)
      mergedConfig.put(key, decorators)
    }
  }

  private def mergeKryoRegister(componentConfigs: Iterable[JMap[AnyRef, AnyRef]],
                                allConfig: JMap[AnyRef, AnyRef],
                                mergedConfig: JMap[AnyRef, AnyRef]): Unit = {
    val key = Config.TOPOLOGY_KRYO_REGISTER
    val configs = getConfigValues(componentConfigs, allConfig, key)
    val merged = configs.foldLeft(Map.empty[String, String]) {
      case (accum, config: JIterable[_]) =>
        accum ++ config.map {
          case m: JMap[_, _] =>
            m.map {
              case (k: String, v: String) => k ->v
              case illegal =>
                throw new IllegalArgumentException(
                  s"each element of $key must be a String or a Map of Strings; actually $illegal")
            }
          case s: String =>
            Map(s -> null)
          case illegal =>
            throw new IllegalArgumentException(s"each element of $key must be a String or a Map of Strings; actually $illegal")
        }.reduce(_ ++ _)
      case (accum, null) =>
        accum
      case (accum, illegal) =>
        throw new IllegalArgumentException(
          s"$key must be an Iterable containing only Strings or Maps of Strings; actually $illegal")
    }
    if (merged.nonEmpty) {
      val registers: JMap[String, String] = new JHashMap[String, String](merged.size)
      registers.putAll(merged)
     mergedConfig.put(key, registers)
    }
  }

  private def readStormConfig(config: String): JMap[AnyRef, AnyRef] = {
    var ret: JMap[AnyRef, AnyRef] = new JHashMap[AnyRef, AnyRef]
    try {
      val yaml = new Yaml(new SafeConstructor)
      val input: InputStream = new FileInputStream(config)
      try {
        ret = yaml.load(new InputStreamReader(input)).asInstanceOf[JMap[AnyRef, AnyRef]]
      } catch {
        case e: IOException =>
          LOG.error(s"failed to load config file $config")
      } finally {
        input.close()
      }
    } catch {
      case e: FileNotFoundException =>
        LOG.error(s"failed to find config file $config")
      case t: Throwable =>
        LOG.error(t.getMessage)
    }
    ret
  }
}

object GearpumpStormTopology {
  private val LOG: Logger = LogUtil.getLogger(classOf[GearpumpStormTopology])
}
