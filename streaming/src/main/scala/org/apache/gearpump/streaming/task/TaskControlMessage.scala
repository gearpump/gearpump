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

package org.apache.gearpump.streaming.task

import org.apache.gearpump.TimeStamp


/*
 * Initial AckRequest
 */
case class InitialAckRequest(taskId: TaskId, sessionId: Int)

/*
  Here the sessionId filed is used to distinguish messages
    between different replays after the application restart
 */
case class AckRequest(taskId: TaskId, seq: Short, sessionId: Int)
/*
  Here the seq field represents the expected number of received messages
    and the actualReceivedNum field means the actual received number since start
 */
case class Ack(taskId: TaskId, seq: Short, actualReceivedNum: Short, sessionId: Int)


sealed trait ClockEvent

case class UpdateClock(taskId: TaskId, time: TimeStamp) extends ClockEvent

object GetLatestMinClock extends ClockEvent

case class GetUpstreamMinClock(taskId: TaskId) extends ClockEvent

case class UpstreamMinClock(latestMinClock: TimeStamp)

case class LatestMinClock(clock: TimeStamp)

case class ReportCheckpointClock(taskId: TaskId, clock: TimeStamp)

object GetStartClock

case class StartClock(clock: TimeStamp)

case class LatencyProbe(timestamp: Long)

case class SendMessageLoss()
