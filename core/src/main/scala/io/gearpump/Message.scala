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

trait Message {

  val value: Any

  val timestamp: Instant
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