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

import java.util.{HashMap => JHashMap, List => JList, Map => JMap}

import akka.actor.ActorSystem
import backtype.storm.generated.{ComponentCommon, StormTopology}
import backtype.storm.tuple.Fields
import backtype.storm.utils.Utils
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.util.LogUtil
import org.json.simple.JSONValue

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object StormUtil {
  val TOPOLOGY = "topology"
  val PROCESSOR_TO_COMPONENT = "processor_to_component"
  val STORM_CONFIG = "storm_config"
  val STORM_CONTEXT = "storm_context"

  private val LOG = LogUtil.getLogger(StormUtil.getClass)

  private val stormConfig = Utils.readStormConfig().asInstanceOf[JMap[AnyRef, AnyRef]]



  def getTopology(config: UserConfig)(implicit system: ActorSystem) : StormTopology = {
    config.getValue[StormTopology](TOPOLOGY)
      .getOrElse(throw new RuntimeException("storm topology is not found"))
  }

  def getStormConfig(config: UserConfig)(implicit system: ActorSystem) : JMap[_, _] = {
    val serConf = config.getValue[String](STORM_CONFIG)
      .getOrElse(throw new RuntimeException("storm config not found"))
    val conf = JSONValue.parse(serConf).asInstanceOf[JMap[AnyRef, AnyRef]]
    stormConfig.putAll(conf)
    stormConfig
  }

  def getComponentToStreamFields(topology: StormTopology): JMap[String, JMap[String, Fields]] = {
    val spouts = topology.get_spouts()
    val bolts = topology.get_bolts()

    (spouts.map{ case (id, component) =>
      id -> getComponentToFields(component.get_common())
    } ++
      bolts.map { case (id, component) =>
        id -> getComponentToFields(component.get_common())
      }).toMap.asJava
  }

  def getComponentToFields(common: ComponentCommon): JMap[String, Fields] = {
    common.get_streams.map { case (sid, stream) =>
      sid -> new Fields(stream.get_output_fields())
    }.toMap.asJava
  }
}
