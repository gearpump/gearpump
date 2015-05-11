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

import org.apache.gearpump._

/**
 * PersistentState is part of the transaction API
 *
 * Users could get transaction support from the framework by
 * conforming to PersistentState APIs and extending PersistentTask
 * to manage the state
 */
trait PersistentState[T] {

  /**
   * recover state to a previous checkpoint
   * usually invoked by the framework
   */
  def recover(timestamp: TimeStamp, bytes: Array[Byte]): Unit

  /**
   * update state on a new message
   * this is invoked by user
   */
  def update(timestamp: TimeStamp, t: T, checkpointTime: TimeStamp): Unit

  /**
   * get a binary snapshot of state
   * usually invoked by the framework
   */
  def checkpoint(checkpointTime: TimeStamp): Array[Byte]

  /**
   * unwrap the raw value of state
   */
  def get: Option[T]
}


