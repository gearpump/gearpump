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

import org.apache.gearpump.Message

/**
 * StateOp api defines operation on State
 * Users need to extends this api to implement
 * their own logic
 *
 * e.g. to count the number of messages
 *
 *  new StateOp[Long] {
 *
 *    def init: Long = 0
 *
 *    def update(msg: Message, t: Long): Long = t + 1
 *
 *    def serialize(t: Long): Array[Byte] = // serialize long value
 *
 *    def deserialize(bytes: Array[Byte]) = // deserialize bytes to long
 *  }
 * @tparam T
 */
trait StateOp[T] {
  /**
   * give state an initial value
   */
  def init: T

  /**
   * update state on new messages
   * @param msg
   * @param t
   */
  def update(msg: Message, t: T): T

  def serialize(t: T): Array[Byte]

  def deserialize(bytes: Array[Byte]): T
}
