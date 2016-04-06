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

package io.gearpump.experiments.storm.processor

import java.util.{Collection => JCollection, List => JList}

import backtype.storm.task.IOutputCollector
import backtype.storm.tuple.Tuple

import io.gearpump.experiments.storm.topology.TimedTuple
import io.gearpump.experiments.storm.util.StormConstants._
import io.gearpump.experiments.storm.util.StormOutputCollector
import io.gearpump.streaming.task.ReportCheckpointClock

/**
 * this is used by Storm bolt to emit messages
 */
private[storm] class StormBoltOutputCollector(collector: StormOutputCollector,
    ackEnabled: Boolean = false) extends IOutputCollector {
  private var reportTime = 0L
  private var maxAckTime = 0L

  override def emit(
      streamId: String, anchors: JCollection[Tuple], tuple: JList[AnyRef]): JList[Integer] = {
    collector.emit(streamId, tuple)
  }

  override def emitDirect(
      taskId: Int, streamId: String, anchors: JCollection[Tuple], tuple: JList[AnyRef]): Unit = {
    collector.emitDirect(taskId, streamId, tuple)
  }

  override def fail(tuple: Tuple): Unit = {
    // application failure, throw exception such that the tuple can be replayed
    // Note: do not print the tuple which will trigger NPE since its messageId is null
    throw new Exception("Storm Bolt.execute failed")
  }

  override def ack(tuple: Tuple): Unit = {
    if (ackEnabled) {
      tuple match {
        case timedTuple: TimedTuple =>
          maxAckTime = Math.max(maxAckTime, timedTuple.timestamp)
          val taskContext = collector.taskContext
          val upstreamMinClock = taskContext.upstreamMinClock
          if (reportTime <= upstreamMinClock && upstreamMinClock <= maxAckTime) {
            reportTime = upstreamMinClock / CHECKPOINT_INTERVAL_MILLIS * CHECKPOINT_INTERVAL_MILLIS
            taskContext.appMaster ! ReportCheckpointClock(taskContext.taskId, reportTime)
            reportTime += CHECKPOINT_INTERVAL_MILLIS
          }
        case _ =>
        // ignore other tuples
      }
    }
  }

  override def reportError(throwable: Throwable): Unit = {
    throw throwable
  }
}
