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

package io.gearpump.streaming.state.impl

import io.gearpump.Time.MilliSeconds
import io.gearpump.streaming.state.api.{Monoid, MonoidState, Serializer}
import io.gearpump.streaming.state.impl.NonWindowState._
import io.gearpump.util.LogUtil
import org.slf4j.Logger

object NonWindowState {
  val LOG: Logger = LogUtil.getLogger(classOf[NonWindowState[_]])
}

/**
 * a MonoidState storing non-window state
 */
class NonWindowState[T](monoid: Monoid[T], serializer: Serializer[T])
  extends MonoidState[T](monoid) {

  override def recover(timestamp: MilliSeconds, bytes: Array[Byte]): Unit = {
    serializer.deserialize(bytes).foreach(left = _)
  }

  override def update(timestamp: MilliSeconds, t: T): Unit = {
    updateState(timestamp, t)
  }

  override def checkpoint(): Array[Byte] = {
    val serialized = serializer.serialize(left)
    LOG.debug(s"checkpoint time: $checkpointTime; checkpoint value: ($checkpointTime, $left)")
    left = monoid.plus(left, right)
    right = monoid.zero
    serialized
  }
}
