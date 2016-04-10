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

package io.gearpump.experiments.storm.topology

import java.util.{List => JList}

import backtype.storm.task.GeneralTopologyContext
import backtype.storm.tuple.{Tuple, TupleImpl}

import io.gearpump.TimeStamp


/**
 * this carries Storm tuple values in the Gearpump world
 * the targetPartitions field dictate which tasks a GearpumpTuple should be sent to
 * see [[io.gearpump.experiments.storm.partitioner.StormPartitioner]] for more info
 */
private[storm] class GearpumpTuple(
    val values: JList[AnyRef],
    val sourceTaskId: Integer,
    val sourceStreamId: String,
    @transient val targetPartitions: Map[String, Array[Int]]) extends Serializable {
  /**
   * creates a Storm [[backtype.storm.tuple.Tuple]] to be passed to a Storm component
   * this is needed for each incoming message
   * because we cannot get [[backtype.storm.task.GeneralTopologyContext]] at deserialization
   * @param topologyContext topology context used for all tasks
   * @return a Tuple
   */
  def toTuple(topologyContext: GeneralTopologyContext, timestamp: TimeStamp): Tuple = {
    TimedTuple(topologyContext, values, sourceTaskId, sourceStreamId, timestamp)
  }


  def canEqual(other: Any): Boolean = other.isInstanceOf[GearpumpTuple]

  override def equals(other: Any): Boolean = other match {
    case that: GearpumpTuple =>
      (that canEqual this) &&
        values == that.values &&
        sourceTaskId == that.sourceTaskId &&
        sourceStreamId == that.sourceStreamId
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(values, sourceTaskId, sourceStreamId)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

case class TimedTuple(topologyContext: GeneralTopologyContext, tuple: JList[AnyRef],
    sourceTaskId: Integer, sourceStreamId: String, timestamp: TimeStamp)
  extends TupleImpl(topologyContext, tuple, sourceTaskId, sourceStreamId, null)


