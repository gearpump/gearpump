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

package io.gearpump.experiments.storm.util

import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import backtype.storm.generated.{ComponentCommon, Grouping, StormTopology}
import backtype.storm.tuple.{Fields, Tuple}
import io.gearpump._
import io.gearpump.experiments.storm.util.StormUtil._
import io.gearpump.streaming.task.TaskContext
import io.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.collection.JavaConversions._

object StormOutputCollector {
  private val LOG: Logger = LogUtil.getLogger(classOf[StormOutputCollector])
  private val EMPTY_LIST: JList[Integer] = new JArrayList[Integer](0)
}

class StormOutputCollector(
    context: TaskContext,
    topology: StormTopology,
    componentId: String,
    componentCommon: ComponentCommon,
    stormTaskId: Int,
    taskToComponent: Map[Integer, String],
    componentToSortedTasks: JMap[String, JList[Integer]],
    targets: JMap[String, JMap[String, Grouping]]) {
  import io.gearpump.experiments.storm.util.StormOutputCollector._

  private var timestamp: TimeStamp = LatestTime
  private val taskToPartition = taskToComponent.map { case (task, component) =>
    task -> componentToSortedTasks.get(component).indexOf(task)
  }

  def emit(streamId: String, values: JList[AnyRef]): JList[Integer] = {
    if (targets.containsKey(streamId)) {
      val outFields = new Fields(componentCommon.get_streams().get(streamId).get_output_fields())
      val targetPartitions = targets.get(streamId).foldLeft(Map.empty[String, Int]) { case (accum, iter) =>
        val (target, grouping) = iter
        if (grouping.getSetField == Grouping._Fields.ALL) {
          accum
        } else {
          val parallelism = getComponentParallelism(target, topology)
          val grouper = getGrouper(outFields, grouping, parallelism)
          val partition = grouper.getPartition(stormTaskId, values)
          accum + (target -> partition)
        }
      }
      val tuple = StormTuple(values, componentId, streamId, stormTaskId, targetPartitions)
      context.output(Message(tuple, timestamp))
      targetPartitions.toList.map { case (target, partition) =>
        componentToSortedTasks.get(target).get(partition)
      }
    } else {
      EMPTY_LIST
    }
  }

  def emitDirect(id: Int, streamId: String, values: JList[AnyRef]): Unit = {
    if (targets.containsKey(streamId)) {
      val target = taskToComponent(id)
      val partition = taskToPartition(id)
      val targetPartitions = Map(target -> partition)
      val tuple = StormTuple(values, componentId, streamId, stormTaskId, targetPartitions)
      context.output(Message(tuple, timestamp))
    }
  }

  def fail(tuple: Tuple): Unit = {}

  def ack(tuple: Tuple): Unit = {}


  def setTimestamp(timestamp: TimeStamp): Unit = {
    this.timestamp = timestamp
  }

}
