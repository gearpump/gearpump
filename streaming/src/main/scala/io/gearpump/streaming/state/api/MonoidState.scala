/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.streaming.state.api

import io.gearpump.Time.MilliSeconds

/**
 * MonoidState uses Algebird Monoid to aggregate state
 *
 * on start, state value is initialized to monoid.zero
 * on each new message, existing state value is aggregated with
 * the incoming value using monoid.plus to get a new state value
 */
abstract class MonoidState[T](monoid: Monoid[T]) extends PersistentState[T] {
  // Left state updated by messages before checkpoint time
  private[state] var left: T = monoid.zero
  // Right state updated by message after checkpoint time
  private[state] var right: T = monoid.zero

  protected var checkpointTime = Long.MaxValue

  override def get: Option[T] = Option(monoid.plus(left, right))

  override def setNextCheckpointTime(nextCheckpointTime: MilliSeconds): Unit = {
    checkpointTime = nextCheckpointTime
  }

  protected def updateState(timestamp: MilliSeconds, t: T): Unit = {
    if (timestamp < checkpointTime) {
      left = monoid.plus(left, t)
    } else {
      right = monoid.plus(right, t)
    }
  }
}

