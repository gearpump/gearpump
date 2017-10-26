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

package org.apache.gearpump.streaming.task

import org.apache.gearpump.Time.MilliSeconds
import org.apache.gearpump.streaming.ProcessorId

/*
 * Initial AckRequest
 */
case class InitialAckRequest(taskId: TaskId, sessionId: Int)

/*
  Here the sessionId filed is used to distinguish messages
    between different replays after the application restart
 */
case class AckRequest(taskId: TaskId, seq: Short, sessionId: Int, watermark: Long)

/**
 * Ack back to sender task actor.
 *
 * @param seq The seq field represents the expected number of received messages and the
 *            actualReceivedNum field means the actual received number since start.
 */
case class Ack(
    taskId: TaskId, seq: Short, actualReceivedNum: Short, sessionId: Int, watermark: Long)

sealed trait ClockEvent

case class UpdateClock(taskId: TaskId, time: MilliSeconds) extends ClockEvent

object GetLatestMinClock extends ClockEvent

case class GetUpstreamMinClock(taskId: TaskId) extends ClockEvent

case class UpdateCheckpointClock(taskId: TaskId, clock: MilliSeconds) extends ClockEvent

case object GetCheckpointClock extends ClockEvent

case class CheckpointClock(clock: Option[MilliSeconds])

case class UpstreamMinClock(latestMinClock: Option[MilliSeconds])

case class LatestMinClock(clock: MilliSeconds)

case object GetStartClock

case class StartClock(clock: MilliSeconds)

case object EndingClock

/** Probe the latency between two upstream to downstream tasks. */
case class LatencyProbe(timestamp: Long)

case object SendMessageLoss

case object GetDAG

case class CheckProcessorDeath(processorId: ProcessorId)
