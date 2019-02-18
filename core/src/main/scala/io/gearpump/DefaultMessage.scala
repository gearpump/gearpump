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

package io.gearpump

import io.gearpump.Time.MilliSeconds
import java.time.Instant

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
