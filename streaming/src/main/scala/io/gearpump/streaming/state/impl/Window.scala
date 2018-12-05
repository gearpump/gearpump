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

/**
 * Used in window applications
 * it keeps the current window and slide ahead when the window expires
 */
class Window(val windowSize: Long, val windowStep: Long) {

  def this(windowConfig: WindowConfig) = {
    this(windowConfig.windowSize, windowConfig.windowStep)
  }

  private var clock: MilliSeconds = 0L
  private var startTime = 0L

  def update(clock: MilliSeconds): Unit = {
    this.clock = clock
  }

  def slideOneStep(): Unit = {
    startTime += windowStep
  }

  def slideTo(timestamp: MilliSeconds): Unit = {
    startTime = timestamp / windowStep * windowStep
  }

  def shouldSlide: Boolean = {
    clock >= (startTime + windowSize)
  }

  def range: (MilliSeconds, MilliSeconds) = {
    startTime -> (startTime + windowSize)
  }
}
