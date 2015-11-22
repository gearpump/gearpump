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

object GearpumpStormTopology {
  private val LOG: Logger = LogUtil.getLogger(classOf[GearpumpStormTopology])

  def apply(
      topology: StormTopology,
      appConfigInJson: String,
      fileConfig: String)(implicit system: ActorSystem): GearpumpStormTopology = {
    new GearpumpStormTopology(
      topology,
      Utils.readStormConfig().asInstanceOf[JMap[AnyRef, AnyRef]],
      parseJsonStringToMap(appConfigInJson),
      readStormConfig(fileConfig)
    )

  }

  /**
   * @param configFile user provided local config file
   * @return a config Map loaded from config file
   */
  private def readStormConfig(configFile: String): JMap[AnyRef, AnyRef] = {
    var ret: JMap[AnyRef, AnyRef] = new JHashMap[AnyRef, AnyRef]
    try {
      val yaml = new Yaml(new SafeConstructor)
      val input: InputStream = new FileInputStream(configFile)
      try {
        ret = yaml.load(new InputStreamReader(input)).asInstanceOf[JMap[AnyRef, AnyRef]]
      } catch {
        case e: IOException =>
          LOG.warn(s"failed to load config file $configFile")
      } finally {
        input.close()
      }
    } catch {
      case e: FileNotFoundException =>
        LOG.error(s"failed to find config file $configFile")
      case t: Throwable =>
        LOG.error(t.getMessage)
    }
    ret
  }
}

/**
 * this is a wrapper over Storm topology which
 *   1. merges Storm and Gearpump configs
 *   2. creates Gearpump processors
 *   3. provides interface for Gearpump applications to use Storm topology
 *
 * an implicit ActorSystem is required to create Gearpump processors
 * @param topology Storm topology
 * @param sysConfig configs from "defaults.yaml" and "storm.yaml"
 * @param appConfig config submitted from user application
 * @param fileConfig custom file config set by user in command line
 */
private[storm] class GearpumpStormTopology(
    topology: StormTopology,
    sysConfig: JMap[AnyRef, AnyRef],
    appConfig: JMap[AnyRef, AnyRef],
    fileConfig: JMap[AnyRef, AnyRef])(implicit system: ActorSystem) {

  private val spouts = topology.get_spouts()
  private val bolts = topology.get_bolts()
  private val stormConfig = mergeConfigs(sysConfig, appConfig, fileConfig, getComponentConfigs(spouts, bolts))
  private val spoutProcessors = spouts.map { case (id, spout) =>
    id -> spoutToProcessor(id, spout, stormConfig.toMap) }.toMap
  private val boltProcessors = bolts.map { case (id, bolt) =>
    id -> boltToProcessor(id, bolt, stormConfig.toMap) }.toMap
  private val allProcessors = spoutProcessors ++ boltProcessors

  /**
   * @return merged Storm config with priority
   *    defaults.yaml < storm.yaml < application config < component config < custom file config
   */
  def getStormConfig: JMap[AnyRef, AnyRef] = stormConfig

  /**
   * @return Storm components to Gearpump processors
   */
  def getProcessors: Map[String, Processor[Task]] = allProcessors

  /**
   * @param sourceId source component id
   * @return target Storm components and Gearpump processors
   */
  def getTargets(sourceId: String): Map[String, Processor[Task]] = {
    getTargets(sourceId, topology).map { case (targetId, _) =>
      targetId -> boltProcessors(targetId)
    }
  }

  /**
   * merge configs from application, custom config file and component
   */
  private def mergeConfigs(
      sysConfig: JMap[AnyRef, AnyRef],
      appConfig: JMap[AnyRef, AnyRef],
      fileConfig: JMap[AnyRef, AnyRef],
      componentConfigs: Iterable[JMap[AnyRef, AnyRef]]): JMap[AnyRef, AnyRef] = {
    val allConfig = new JHashMap[AnyRef, AnyRef]
    allConfig.putAll(sysConfig)
    allConfig.putAll(appConfig)
    allConfig.putAll(getMergedComponentConfig(componentConfigs, allConfig.toMap))
    allConfig.putAll(fileConfig)
    allConfig
  }

  /**
   * creates Gearpump processor from Storm spout
   * @param spoutId spout id
   * @param spoutSpec spout spec
   * @param stormConfig merged storm config
   * @param system actor system
   * @return a Processor[StormProducer]
   */
  private def spoutToProcessor(spoutId: String, spoutSpec: SpoutSpec,
      stormConfig: Map[AnyRef, AnyRef])(implicit system: ActorSystem): Processor[Task] = {
    val componentCommon = spoutSpec.get_common()
    val taskConf = UserConfig.empty
        .withString(STORM_COMPONENT, spoutId)
    val parallelism = getParallelism(stormConfig, componentCommon)
    Processor[StormProducer](parallelism, spoutId, taskConf)
  }

  /**
   * creates Gearpump processor from Storm bolt
   * @param boltId bolt id
   * @param boltSpec bolt spec
   * @param stormConfig merged storm config
   * @param system actor system
   * @return a Processor[StormProcessor]
   */
  private def boltToProcessor(boltId: String, boltSpec: Bolt,
      stormConfig: Map[AnyRef, AnyRef])(implicit system: ActorSystem): Processor[Task] = {
    val componentCommon = boltSpec.get_common()
    val taskConf = UserConfig.empty
        .withString(STORM_COMPONENT, boltId)
    val parallelism = getParallelism(stormConfig, componentCommon)
    Processor[StormProcessor](parallelism, boltId, taskConf)
  }

  /**
   * @return target components and streams
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

  /**
   * @return input stream and grouping for a Storm component
   */
  private def getInputs(componentId: String, topology: StormTopology): JMap[GlobalStreamId, Grouping] = {
    ThriftTopologyUtils.getComponentCommon(topology, componentId).get_inputs
  }

  /**
   * get Storm component parallelism according to the following rule,
   *   1. use "topology.tasks" if defined; otherwise use parallelism_hint
   *   2. parallelism should not be larger than "topology.max.task.parallelism" if defined
   *   3. component config overrides system config
   * @param stormConfig system configs without merging "topology.tasks" and "topology.max.task.parallelism" of component
   * @return number of task instances for a component
   */
  private def getParallelism(stormConfig: Map[AnyRef, AnyRef], component: ComponentCommon): Int = {
    val parallelismHint: Int = if (component.is_set_parallelism_hint()) {
      component.get_parallelism_hint()
    } else {
      1
    }
    val mergedConfig = new JHashMap[AnyRef, AnyRef]
    val componentConfig = parseJsonStringToMap(component.get_json_conf)
    mergedConfig.putAll(stormConfig)
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

  /**
   * merge component configs "topology.kryo.decorators" and "topology.kryo.register"
   * @param componentConfigs list of component configs
   * @param allConfig existing configs without merging component configs
   * @return the two configs merged from all the component configs and existing configs
   */
  private def getMergedComponentConfig(componentConfigs: Iterable[JMap[AnyRef, AnyRef]],
                                       allConfig: Map[AnyRef, AnyRef]): JMap[AnyRef, AnyRef] = {
    val mergedConfig: JMap[AnyRef, AnyRef] = new JHashMap[AnyRef, AnyRef]
    mergedConfig.putAll(getMergedKryoDecorators(componentConfigs, allConfig))
    mergedConfig.putAll(getMergedKryoRegister(componentConfigs, allConfig))
    mergedConfig
  }

  /**
   * @param componentConfigs list of component configs
   * @param allConfig existing configs without merging component configs
   * @return a merged config with a list of distinct kryo decorators from component and existing configs
   */
  private def getMergedKryoDecorators(componentConfigs: Iterable[JMap[AnyRef, AnyRef]],
                                allConfig: Map[AnyRef, AnyRef]): JMap[AnyRef, AnyRef] = {
    val mergedConfig: JMap[AnyRef, AnyRef] = new JHashMap[AnyRef, AnyRef](1)
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
    mergedConfig
  }

  /**
   * @param componentConfigs list of component configs
   * @param allConfig existing configs without merging component configs
   * @return a merged config with component config overriding existing configs
   */
  private def getMergedKryoRegister(componentConfigs: Iterable[JMap[AnyRef, AnyRef]],
                              allConfig: Map[AnyRef, AnyRef]): JMap[AnyRef, AnyRef] = {
    val mergedConfig: JMap[AnyRef, AnyRef] = new JHashMap[AnyRef, AnyRef](1)
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
    mergedConfig
  }

  /**
   * @param componentConfigs list of raw component configs
   * @param allConfig existing configs without merging component configs
   * @param key config key
   * @return a list of values for a config from both component configs and existing configs
   */
  private def getConfigValues(componentConfigs: Iterable[JMap[AnyRef, AnyRef]],
                              allConfig: Map[AnyRef, AnyRef], key: String): Iterable[AnyRef] = {
    componentConfigs.map(config => config.get(key)) ++ allConfig.get(key).toList
  }



}

