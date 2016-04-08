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

package io.gearpump.streaming.transaction.api

import scala.util.Try

import io.gearpump.TimeStamp

object OffsetStorage {

  /**
   * StorageEmpty means no data has been stored
   */
  case object StorageEmpty extends Throwable

  /**
   * Overflow means the looked up time is
   * larger than the maximum stored TimeStamp
   */
  case class Overflow(maxTimestamp: Array[Byte]) extends Throwable

  /**
   * Underflow means the looked up time is
   * smaller than the minimum stored TimeStamp
   */
  case class Underflow(minTimestamp: Array[Byte]) extends Throwable
}

/**
 * OffsetStorage stores the mapping from TimeStamp to Offset
 */
trait OffsetStorage {
  /**
   * try to look up the time in the OffsetStorage return the corresponding Offset if the time is
   * in the range of stored TimeStamps or one of the failure info (StorageEmpty, Overflow,
   * Underflow)
   *
   * @param time the time to look for
   * @return the corresponding offset if the time is in the range, otherwise failure
   */
  def lookUp(time: TimeStamp): Try[Array[Byte]]

  def append(time: TimeStamp, offset: Array[Byte]): Unit

  def close(): Unit
}

trait OffsetStorageFactory extends java.io.Serializable {
  def getOffsetStorage(dir: String): OffsetStorage
}