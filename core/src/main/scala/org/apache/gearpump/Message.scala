/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package org.apache.gearpump

import java.time.Instant

/**
 * Each message contains an immutable timestamp.
 *
 * For example, if you take a picture, the time you take the picture is the
 * message's timestamp.
 *
 * @param msg Accept any type except Null, Nothing and Unit
 */
case class Message(msg: Any, timeInMillis: TimeStamp) {

  /**
   * @param msg Accept any type except Null, Nothing and Unit
   * @param timestamp timestamp cannot be larger than Instant.ofEpochMilli(Long.MaxValue)
   */
  def this(msg: Any, timestamp: Instant) = {
    this(msg, timestamp.toEpochMilli)
  }

  /**
   * Instant.EPOCH is used for default timestamp
   *
   * @param msg Accept any type except Null, Nothing and Uni
   */
  def this(msg: Any) = {
    this(msg, Instant.EPOCH)
  }

  def timestamp: Instant = {
    Instant.ofEpochMilli(timeInMillis)
  }
}

object Message {

  /**
   * Instant.EPOCH is used for default timestamp
   *
   * @param msg Accept any type except Null, Nothing and Uni
   */
  def apply(msg: Any): Message = {
    new Message(msg)
  }

  /**
   * @param msg Accept any type except Null, Nothing and Unit
   * @param timestamp timestamp cannot be larger than Instant.ofEpochMilli(Long.MaxValue)
   */
  def apply(msg: Any, timestamp: Instant): Message = {
    new Message(msg, timestamp)
  }
}
