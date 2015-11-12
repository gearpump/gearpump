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

import java.util.{ArrayList => JArrayList, Iterator => JIterator, List => JList, Map => JMap}

import backtype.storm.generated.{GlobalStreamId, Grouping, JavaObject}
import backtype.storm.grouping.CustomStreamGrouping
import backtype.storm.task.TopologyContext
import backtype.storm.tuple.Fields
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
  private[storm] val EMPTY_LIST: JList[Integer] = new JArrayList[Integer](0)

  def apply(taskContext: TaskContext, topologyContext: TopologyContext): StormOutputCollector = {
    val stormTaskId = topologyContext.getThisTaskId
    val componentId = topologyContext.getThisComponentId
    val taskToComponent = topologyContext.getTaskToComponent
    val componentToProcessorId = getComponentToProcessorId(taskToComponent.toMap)
    val targets = topologyContext.getTargets(componentId)
    val streamGroupers: Map[String, Grouper] =
      targets.flatMap { case (streamId, targetGrouping) =>
        targetGrouping.collect { case (target, grouping) if !grouping.is_set_direct() =>
          streamId -> getGrouper(topologyContext, grouping, componentId, streamId, target)
        }
      }.toMap
    val getTargetPartitionsFn = (streamId: String, values: JList[AnyRef]) => {
      getTargetPartitions(stormTaskId, streamId, targets, streamGroupers, componentToProcessorId, values)
    }
    new StormOutputCollector(stormTaskId, taskToComponent, targets, getTargetPartitionsFn, taskContext, LatestTime)
  }

  /**
   * get target Gearpump partitions and Storm task ids
   */
  private def getTargetPartitions(
      stormTaskId: Int,
      streamId: String,
      targets: JMap[String, JMap[String, Grouping]],
      streamGroupers: Map[String, Grouper],
      componentToProcessorId: Map[String, ProcessorId],
      values: JList[AnyRef]): (Map[String, List[Int]], JList[Integer]) ={
    val ret: JList[Integer] = new JArrayList[Integer](targets.size)

    @annotation.tailrec
    def getRecur(iter: JIterator[String],
        accum: Map[String, List[Int]]): Map[String, List[Int]] = {
      if (iter.hasNext) {
        val target = iter.next
        val grouper = streamGroupers(streamId)
        val partitions = grouper.getPartitions(stormTaskId, values)
        partitions.foreach { p =>
          val stormTaskId = gearpumpTaskIdToStorm(TaskId(componentToProcessorId(target), p))
          ret.add(stormTaskId)
        }
        getRecur(iter, accum + (target -> partitions))
      } else {
        accum
      }
    }
    val targetPartitions = getRecur(targets.get(streamId).keySet().iterator, Map.empty[String, List[Int]])
    (targetPartitions, ret)
  }

  private def getComponentToProcessorId(taskToComponent: Map[Integer, String]): Map[String, ProcessorId] = {
    taskToComponent.map { case (id, component) =>
      component -> stormTaskIdToGearpump(id).processorId
    }
  }

  private def getGrouper(topologyContext: TopologyContext, grouping: Grouping,
                         source: String, streamId: String, target: String): Grouper = {
    val outFields = topologyContext.getComponentOutputFields(source, streamId)
    val targetTasks = topologyContext.getComponentTasks(target)
    val targetTaskNum = targetTasks.size
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
        grouper.prepare(topologyContext, globalStreamId, targetTasks)
        grouper
      case Grouping._Fields.CUSTOM_OBJECT =>
        val customObject = grouping.get_custom_object()
        val customGrouping = instantiateJavaObject(customObject)
        val grouper = new CustomGrouper(customGrouping)
        grouper.prepare(topologyContext, globalStreamId, targetTasks)
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

/**
 * this provides common functionality for [[io.gearpump.experiments.storm.producer.StormSpoutOutputCollector]]
 * and [[io.gearpump.experiments.storm.processor.StormBoltOutputCollector]]
 */
class StormOutputCollector(
    stormTaskId: Int,
    taskToComponent: JMap[Integer, String],
    targets: JMap[String, JMap[String, Grouping]],
    getTargetPartitionsFn: (String, JList[AnyRef]) => (Map[String, List[Int]], JList[Integer]),
    val taskContext: TaskContext,
    private var timestamp: TimeStamp) {
  import io.gearpump.experiments.storm.util.StormOutputCollector._

  /**
   * this is invoked by a Storm output collector to emit tuple values into a stream.
   * We will wrap the values into a message of [[GearpumpTuple]] along with the target partitions
   * to tell [[io.gearpump.experiments.storm.partitioner.StormPartitioner]] where to send the message.
   * We also return the corresponding target Storm task ids back to the collector
   *
   * @param streamId Storm stream id
   * @param values Storm tuple values
   * @return target Storm task ids
   */
  def emit(streamId: String, values: JList[AnyRef]): JList[Integer] = {
    if (targets.containsKey(streamId)) {
      val (targetPartitions, targetStormTaskIds) = getTargetPartitionsFn(streamId, values)
      val tuple = new GearpumpTuple(values, stormTaskId, streamId, targetPartitions)
      taskContext.output(Message(tuple, timestamp))
      targetStormTaskIds
    } else {
      EMPTY_LIST
    }
  }

  /**
   * this is invoked by Storm output collector to emit tuple values to a specific Storm task.
   * we translate the Storm task id into Gearpump TaskId and tell
   * [[io.gearpump.experiments.storm.partitioner.StormPartitioner]] through the targetPartitions
   * field of [[GearpumpTuple]]
   *
   * @param id Storm task id
   * @param streamId Storm stream id
   * @param values Storm tuple values
   */
  def emitDirect(id: Int, streamId: String, values: JList[AnyRef]): Unit = {
    if (targets.containsKey(streamId)) {
      val target = taskToComponent.get(id)
      val partition = stormTaskIdToGearpump(id).index
      val targetPartitions = Map(target -> List(partition))
      val tuple = new GearpumpTuple(values, stormTaskId, streamId, targetPartitions)
      taskContext.output(Message(tuple, timestamp))
    }
  }

  /**
   * get timestamp from each incoming Message and
   * which will be set into output messages
   */
  def setTimestamp(timestamp: TimeStamp): Unit = {
    this.timestamp = timestamp
  }

  def getTimestamp: Long = timestamp
}
