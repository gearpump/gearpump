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

import org.apache.gearpump.Time.MilliSeconds

trait Message {

  val value: Any

  val timestamp: Instant
}

/**
 * Each message contains an immutable timestamp.
 *
 * For example, if you take a picture, the time you take the picture is the
 * message's timestamp.
 *
 * @param value Accept any type except Null, Nothing and Unit
 */
case class DefaultMessage(value: Any, timeInMillis: MilliSeconds) extends Message {

  /**
   * @param value Accept any type except Null, Nothing and Unit
   * @param timestamp timestamp cannot be larger than Instant.ofEpochMilli(Long.MaxValue)
   */
  def this(value: Any, timestamp: Instant) = {
    this(value, timestamp.toEpochMilli)
  }

  /**
   * Instant.EPOCH is used for default timestamp
   *
   * @param value Accept any type except Null, Nothing and Uni
   */
  def this(value: Any) = {
    this(value, Instant.EPOCH)
  }

  override val timestamp: Instant = {
    Instant.ofEpochMilli(timeInMillis)
  }
}

object Message {

  /**
   * Instant.EPOCH is used for default timestamp
   *
   * @param value Accept any type except Null, Nothing and Unit
   */
  def apply(value: Any): Message = {
    new DefaultMessage(value)
  }

  /**
   * @param value Accept any type except Null, Nothing and Unit
   * @param timestamp timestamp must be smaller than Long.MaxValue
   */
  def apply(value: Any, timestamp: MilliSeconds): Message = {
    DefaultMessage(value, timestamp)
  }

  /**
   * @param value Accept any type except Null, Nothing and Unit
   * @param timestamp timestamp must be smaller than Instant.ofEpochMilli(Long.MaxValue)
   */
  def apply(value: Any, timestamp: Instant): Message = {
    new DefaultMessage(value, timestamp)
  }
}
