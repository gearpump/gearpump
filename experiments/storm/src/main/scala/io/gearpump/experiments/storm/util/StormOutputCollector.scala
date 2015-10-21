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

import java.util.{ArrayList => JArrayList, List => JList, Iterator => JIterator}

import backtype.storm.generated.{JavaObject, GlobalStreamId, Grouping}
import backtype.storm.grouping.CustomStreamGrouping
import backtype.storm.task.TopologyContext
import backtype.storm.tuple.{Fields, Tuple}
import backtype.storm.utils.Utils
import io.gearpump._
import io.gearpump.experiments.storm.topology.GearpumpTuple
import io.gearpump.experiments.storm.util.StormUtil._
import io.gearpump.streaming.ProcessorId
import io.gearpump.streaming.task.{TaskContext, TaskId}
import io.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.collection.JavaConversions._

object StormOutputCollector {
  private val LOG: Logger = LogUtil.getLogger(classOf[StormOutputCollector])
  private val EMPTY_LIST: JList[Integer] = new JArrayList[Integer](0)
}

class StormOutputCollector(
    context: TaskContext,
    topologyContext: TopologyContext) {
  import io.gearpump.experiments.storm.util.StormOutputCollector._

  private val stormTaskId = topologyContext.getThisTaskId
  private val componentId = topologyContext.getThisComponentId
  private var timestamp: TimeStamp = LatestTime
  private val taskToComponent = topologyContext.getTaskToComponent
  private val componentToProcessorId = getComponentToProcessorId(taskToComponent.toMap)
  private val targets = topologyContext.getTargets(componentId)
  private val streamGroupers: Map[String, Grouper] =
    targets.flatMap { case (streamId, targetGrouping) =>
      targetGrouping.collect { case (target, grouping) if !grouping.is_set_direct() =>
        streamId -> getGrouper(topologyContext, grouping, componentId, streamId, target)
      }
    }.toMap

  def emit(streamId: String, values: JList[AnyRef]): JList[Integer] = {
    if (targets.containsKey(streamId)) {
      val ret: JList[Integer] = new JArrayList[Integer](targets.size)

      @annotation.tailrec
      def getTargetPartitions(iter: JIterator[String],
                              accum: Map[String, List[Int]]): Map[String, List[Int]] = {
        if (iter.hasNext) {
          val target = iter.next
          val grouper = streamGroupers(streamId)
          val partitions = grouper.getPartitions(stormTaskId, values)
          partitions.foreach { p =>
            val stormTaskId = gearpumpTaskIdToStorm(TaskId(componentToProcessorId(target), p))
            ret.add(stormTaskId)
          }
          getTargetPartitions(iter, accum + (target -> partitions))
        } else {
          accum
        }

      }

      val targetPartitions = getTargetPartitions(targets.get(streamId).keySet().iterator, Map.empty[String, List[Int]])
      val tuple = new GearpumpTuple(values, stormTaskId, streamId, targetPartitions)
      context.output(Message(tuple, timestamp))
      ret
    } else {
      EMPTY_LIST
    }
  }

  def emitDirect(id: Int, streamId: String, values: JList[AnyRef]): Unit = {
    if (targets.containsKey(streamId)) {
      val target = taskToComponent(id)
      val partition = stormTaskIdToGearpump(id).index
      val targetPartitions = Map(target -> List(partition))
      val tuple = new GearpumpTuple(values, stormTaskId, streamId, targetPartitions)
      context.output(Message(tuple, timestamp))
    }
  }

  def fail(tuple: Tuple): Unit = {
    // TODO: add meaningful semantics
  }


  def ack(tuple: Tuple): Unit = {
    // TODO: add meaningful semantics
  }


  def setTimestamp(timestamp: TimeStamp): Unit = {
    this.timestamp = timestamp
  }

  private def getComponentToProcessorId(taskToComponent: Map[Integer, String]): Map[String, ProcessorId] = {
    taskToComponent.map { case (id, component) =>
      component -> stormTaskIdToGearpump(id).processorId
    }
  }

  private def getGrouper(topologyContext: TopologyContext, grouping: Grouping,
      source: String, streamId: String, target: String): Grouper = {
    val outFields = topologyContext.getComponentOutputFields(source, streamId)
    val sourceTasks = topologyContext.getComponentTasks(source)
    val targetTaskNum = topologyContext.getComponentTasks(target).size
    val globalStreamId = new GlobalStreamId(source, streamId)

    grouping.getSetField match {
      case Grouping._Fields.FIELDS =>
        if (isGlobalGrouping(grouping)) {
          new GlobalGrouper
        } else {
          new FieldsGrouper(outFields, new Fields(grouping.get_fields()), targetTaskNum)
        }
      case Grouping._Fields.SHUFFLE =>
        new ShuffleGrouper(targetTaskNum)
      case Grouping._Fields.NONE =>
        new NoneGrouper(targetTaskNum)
      case Grouping._Fields.ALL =>
        new AllGrouper(targetTaskNum)
      case Grouping._Fields.CUSTOM_SERIALIZED =>
        val customGrouping = Utils.deserialize(grouping.get_custom_serialized()).asInstanceOf[CustomStreamGrouping]
        val grouper = new CustomGrouper(customGrouping)
        grouper.prepare(topologyContext, globalStreamId, sourceTasks)
        grouper
      case Grouping._Fields.CUSTOM_OBJECT =>
        val customObject = grouping.get_custom_object()
        val customGrouping = instantiateJavaObject(customObject)
        val grouper = new CustomGrouper(customGrouping)
        grouper.prepare(topologyContext, globalStreamId, sourceTasks)
        grouper
      case Grouping._Fields.LOCAL_OR_SHUFFLE =>
        // GearPump has built-in support for sending messages to local actor
        new ShuffleGrouper(targetTaskNum)
      case Grouping._Fields.DIRECT =>
        throw new Exception("direct grouping should not be called here")
    }
  }

  private def isGlobalGrouping(grouping: Grouping): Boolean = {
    grouping.getSetField == Grouping._Fields.FIELDS &&
        grouping.get_fields.isEmpty
  }

  private def instantiateJavaObject(javaObject: JavaObject): CustomStreamGrouping = {
    val className = javaObject.get_full_class_name()
    val args = javaObject.get_args_list().map(_.getFieldValue)
    val customGrouping = Class.forName(className).getConstructor(args.map(_.getClass): _*)
        .newInstance(args).asInstanceOf[CustomStreamGrouping]
    customGrouping
  }
}
