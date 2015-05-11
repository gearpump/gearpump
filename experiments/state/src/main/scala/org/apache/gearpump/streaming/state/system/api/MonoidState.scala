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

package org.apache.gearpump.streaming.state.system.api

import com.twitter.algebird.Monoid
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.streaming.state.system.impl.StateSerializer

/**
 * MonoidState uses Algebird Monoid to aggregate state
 *
 * on start, state value is initialized to monoid.zero
 * on each new message, existing state value is aggregated with
 * the incoming value using monoid.plus to get a new state value
 */
abstract class MonoidState[T](monoid: Monoid[T]) extends PersistentState[T] {
  // left state updated by messages before checkpoint time
  protected var left: T = monoid.zero
  // right state updated by message after checkpoint time
  protected var right: T = monoid.zero

  override def get: Option[T] = Option(monoid.plus(left, right))

  override def update(timestamp: TimeStamp, t: T, checkpointTime: TimeStamp): Unit = {
    updateState(timestamp, t, checkpointTime)
  }

  override def checkpoint(checkpointTime: TimeStamp): Array[Byte] = {
    val serialized = StateSerializer.serialize[T](left)
    left = monoid.plus(left, right)
    right = monoid.zero
    serialized
  }

  protected def updateState(timestamp: TimeStamp, t: T, checkpointTime: TimeStamp): Unit = {
    if (timestamp < checkpointTime) {
      left = monoid.plus(left, t)
    } else {
      right = monoid.plus(right, t)
    }
  }
}

