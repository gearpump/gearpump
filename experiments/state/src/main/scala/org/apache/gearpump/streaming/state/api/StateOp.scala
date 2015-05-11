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
 * Users extend this to manipulate state with State API
 */
trait StateOp[T] {
  /**
   *  users should provide a state serializer such that
   *  state could be serialized to / deserialized from
   *  checkpoint store
   */
  def serializer: StateSerializer[T]

  /**
   * Users should define how a message is decoded into raw T
   * and pass it to state.update. Besides, users could call state.get
   * to unwrap the current state or checkpoint to do checkpointing
   * explicitly
   * @param state
   * @param message
   */
  def update(state: State[T], message: Message): Unit
}
