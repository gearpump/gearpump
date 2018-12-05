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
 * PersistentState is part of the transaction API
 *
 * Users could get transaction support from the framework by
 * conforming to PersistentState APIs and extending PersistentTask
 * to manage the state
 */
trait PersistentState[T] {

  /**
   * Recovers state to a previous checkpoint
   * usually invoked by the framework
   */
  def recover(timestamp: MilliSeconds, bytes: Array[Byte]): Unit

  /**
   * Updates state on a new message
   * this is invoked by user
   */
  def update(timestamp: MilliSeconds, t: T): Unit

  /**
   * Sets next checkpoint time
   * should be invoked by the framework
   */
  def setNextCheckpointTime(timeStamp: MilliSeconds): Unit

  /**
   * Gets a binary snapshot of state
   * usually invoked by the framework
   */
  def checkpoint(): Array[Byte]

  /**
   * Unwraps the raw value of state
   */
  def get: Option[T]
}

