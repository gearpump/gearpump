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

package org.apache.gearpump.streaming.state.api

import org.apache.gearpump._

/**
 * State api manages a state lifecycle for users
 * which includes
 *   1. init state at given time
 *   2. update state on new message
 *   3. checkpoint state in the background
 *      if checkpoint time is exceeded (refer to CheckpointManager)
 * State also maintains a clock that advances on checkpoint.
 * Users could get the raw value through unwrap
 *
 * We've already provided two implementations
 *   1. DefaultState for general use
 *   2. FixedWindowState for window functions
 * which take care of state management for users
 * and leave them to focus on operation (refer to StateOp)
 *
 * @tparam T
 */
trait State[T] {

  /**
   * init state at given time
   * @param timestamp
   */
  def init(timestamp: TimeStamp): Unit
  /**
   * update internal state on new message
   * @param msg
   */
  def update(msg: Message): Unit

  /**
   * @return state clock
   */
  def minClock: TimeStamp

  /**
   * unwrap the latest state
   * @return
   */
  def unwrap: Option[T]
}
