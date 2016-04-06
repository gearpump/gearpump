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

import java.lang.{Boolean => JBoolean}
import java.util.{HashMap => JHashMap, Map => JMap}

import akka.actor.ActorSystem
import backtype.storm.Config
import backtype.storm.generated._
import org.apache.storm.shade.org.json.simple.JSONValue

import io.gearpump.cluster.UserConfig
import io.gearpump.experiments.storm.topology.GearpumpStormComponent.{GearpumpBolt, GearpumpSpout}
import io.gearpump.experiments.storm.topology._
import io.gearpump.experiments.storm.util.StormConstants._
import io.gearpump.streaming.task.{TaskContext, TaskId}
import io.gearpump.util.Util

object StormUtil {

  /**
   * Convert storm task id to gearpump [[io.gearpump.streaming.task.TaskId]]
   *
   * The high 16 bit of an Int is TaskId.processorId
   * The low 16 bit of an Int is TaskId.index
   */
  def stormTaskIdToGearpump(id: Integer): TaskId = {
    val index = id & 0xFFFF
    val processorId = id >> 16
    TaskId(processorId, index)
  }

  /**
   * convert gearpump [[TaskId]] to storm task id
   * TaskId.processorId is the high 16 bit of an Int
   * TaskId.index is the low 16 bit  of an Int
   */
  def gearpumpTaskIdToStorm(taskId: TaskId): Integer = {
    val index = taskId.index
    val processorId = taskId.processorId
    (processorId << 16) + (index & 0xFFFF)
  }

  /**
   * @return a configured [[GearpumpStormComponent]]
   */
  def getGearpumpStormComponent(
      taskContext: TaskContext, conf: UserConfig)(implicit system: ActorSystem)
    : GearpumpStormComponent = {
    val topology = conf.getValue[StormTopology](STORM_TOPOLOGY).get
    val stormConfig = conf.getValue[JMap[AnyRef, AnyRef]](STORM_CONFIG).get
    val componentId = conf.getString(STORM_COMPONENT).get
    val spouts = topology.get_spouts
    val bolts = topology.get_bolts
    if (spouts.containsKey(componentId)) {
      GearpumpSpout(topology, stormConfig, spouts.get(componentId), taskContext)
    } else if (bolts.containsKey(componentId)) {
      GearpumpBolt(topology, stormConfig, bolts.get(componentId), taskContext)
    } else {
      throw new Exception(s"storm component $componentId not found")
    }
  }

  /**
   * Parses config in json to map, returns empty map for invalid json string
   *
   * @param json config in json
   * @return config in map
   */
  def parseJsonStringToMap(json: String): JMap[AnyRef, AnyRef] = {
    Option(json).flatMap(json => JSONValue.parse(json) match {
      case m: JMap[_, _] => Option(m.asInstanceOf[JMap[AnyRef, AnyRef]])
      case _ => None
    }).getOrElse(new JHashMap[AnyRef, AnyRef])
  }

  /**
   * get Int value of the config name
   */
  def getInt(conf: JMap[_, _], name: String): Option[Int] = {
    Option(conf.get(name)).map {
      case number: Number => number.intValue
      case invalid => throw new IllegalArgumentException(
        s"$name must be Java Integer; actual: ${invalid.getClass}")
    }
  }

  /**
   * get Boolean value of the config name
   */
  def getBoolean(conf: JMap[_, _], name: AnyRef): Option[Boolean] = {
    Option(conf.get(name)).map {
      case b: JBoolean => b.booleanValue()
      case invalid => throw new IllegalArgumentException(
        s"$name must be a Java Boolean; acutal: ${invalid.getClass}")
    }
  }

  /**
   * clojure mod ported from Storm
   * see https://clojuredocs.org/clojure.core/mod
   */
  def mod(num: Int, div: Int): Int = {
    (num % div + div) % div
  }

  def ackEnabled(config: JMap[AnyRef, AnyRef]): Boolean = {
    if (config.containsKey(Config.TOPOLOGY_ACKER_EXECUTORS)) {
      getInt(config, Config.TOPOLOGY_ACKER_EXECUTORS).map(_ != 0).getOrElse(true)
    } else {
      false
    }
  }

  def getThriftPort(): Int = {
    Util.findFreePort().getOrElse(
      throw new Exception("unable to find free port for thrift server"))
  }
}
