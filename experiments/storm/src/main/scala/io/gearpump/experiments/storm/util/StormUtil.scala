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

import java.util.{HashMap => JHashMap, Map => JMap}

import akka.actor.ActorSystem
import backtype.storm.generated._
import io.gearpump.cluster.UserConfig
import io.gearpump.experiments.storm.topology.GearpumpStormComponent.{GearpumpBolt, GearpumpSpout}
import io.gearpump.experiments.storm.topology._
import io.gearpump.streaming.task.{TaskContext, TaskId}
import org.json.simple.JSONValue

object StormUtil {

  import StormConstants._

  /**
   * convert storm task id to gearpump [[TaskId]]
   * the high 16 bit of an Int is TaskId.processorId
   * the low 16 bit of an Int is TaskId.index
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


  def getGearpumpStormComponent(taskContext: TaskContext, conf: UserConfig)(implicit system: ActorSystem): GearpumpStormComponent = {
    val topology = conf.getValue[StormTopology](STORM_TOPOLOGY).get
    val componentId = conf.getString(STORM_COMPONENT).get
    val spouts = topology.get_spouts
    val bolts = topology.get_bolts
    if (spouts.containsKey(componentId)) {
      GearpumpSpout(topology, componentId, spouts.get(componentId), taskContext)
    } else if (bolts.containsKey(componentId)) {
      GearpumpBolt(topology, componentId, bolts.get(componentId), taskContext)
    } else {
      throw new Exception(s"storm component $componentId not found")
    }
  }

  def parseJsonStringToMap(json: String): JMap[AnyRef, AnyRef] = {
    Option(json).flatMap(json => Option(JSONValue.parse(json)))
        .getOrElse(new JHashMap[AnyRef, AnyRef]).asInstanceOf[JMap[AnyRef, AnyRef]]
  }

}
