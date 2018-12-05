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

package io.gearpump.streaming

import io.gearpump.Time
import io.gearpump.Time.MilliSeconds

/**
 * Each processor has a LifeTime.
 *
 * When input message's timestamp is beyond current processor's lifetime,
 * then it will not be processed by this processor.
 */
case class LifeTime(birth: MilliSeconds, death: MilliSeconds) {
  def contains(timestamp: MilliSeconds): Boolean = {
    timestamp >= birth && timestamp < death
  }

  def cross(another: LifeTime): LifeTime = {
    LifeTime(Math.max(birth, another.birth), Math.min(death, another.death))
  }
}

object LifeTime {
  val Immortal = LifeTime(Time.MIN_TIME_MILLIS, Time.UNREACHABLE)
}