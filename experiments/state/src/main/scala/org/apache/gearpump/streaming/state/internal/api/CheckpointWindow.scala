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

package org.apache.gearpump.streaming.state.internal.api

import com.twitter.algebird.Semigroup
import org.apache.gearpump.TimeStamp

object CheckpointWindow {
  implicit val windowSemigroup = new Semigroup[CheckpointWindow] {
    override def plus(l: CheckpointWindow, r: CheckpointWindow): CheckpointWindow = {
      if (r < l) {
        plus(r, l)
      } else if (l.endTime < r.startTime) {
        throw new RuntimeException(s"cannot merge non-overlapping windows $l and $r")
      } else {
        CheckpointWindow(l.startTime, r.endTime)
      }
    }
  }

  def at(timestamp: TimeStamp, windowSize: Long): CheckpointWindow = {
    val startClock = (timestamp / windowSize) * windowSize
    val endClock = startClock + windowSize
    CheckpointWindow(startClock, endClock)
  }
}
/**
 * time window between checkpoints, ranging
 * from (including)startTime until (excluding)endTime
 * @param startTime timestamp of data in the window is >= startTime
 * @param endTime   timestamp of data in the window is < startTime
 */
private[internal] case class CheckpointWindow(startTime: TimeStamp, endTime: TimeStamp) extends Ordered[CheckpointWindow] {
  override def compare(that: CheckpointWindow): Int = {
      if (startTime < that.startTime) -1
      else if (startTime > that.startTime) 1
      else 0
  }

}

