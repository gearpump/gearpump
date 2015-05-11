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
 * State API provides a container for users to keep their in memory state
 * persistently.
 */
trait State[T] {
  /**
   * set a persistent store that will checkpoint state periodically
   * @param checkpointStore
   */
  def setCheckpointStore(checkpointStore: CheckpointStore): Unit

  /**
   * recover state from checkpointStore at the given timestamp
   * @param timestamp
   */
  def recover(timestamp: TimeStamp): Unit

  /**
   * update state applying the aggregate function
   * @param timestamp
   * @param t
   * @param aggregate
   */
  def update(timestamp: TimeStamp, t: T, aggregate: (T, T) => T): Unit

  /**
   * get current state
   * @return
   */
  def get: Option[T]

  /**
   * clock of the latest checkpointed state
   * @return
   */
  def minClock: TimeStamp

  /**
   * checkpoint state associated with timestamp such that
   * it could be recovered when crashed task restarts
   */
  def checkpoint(timestamp: TimeStamp): Unit
}