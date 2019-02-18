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
package io.gearpump.streaming.dsl.window.api

import io.gearpump.Time
import io.gearpump.Time.MilliSeconds
import io.gearpump.streaming.dsl.window.impl.Window
import java.time.{Duration, Instant}
import scala.collection.mutable.ArrayBuffer

object WindowFunction {

  trait Context[T] {
    def element: T
    def timestamp: Instant
  }
}

/**
 * Determines how elements are assigned to windows for calculation.
 */
trait WindowFunction {

  /**
   * Assigns elements into windows.
   */
  def apply[T](context: WindowFunction.Context[T]): Array[Window]

  def isNonMerging: Boolean
}

abstract class NonMergingWindowFunction extends WindowFunction {

  override def isNonMerging: Boolean = true
}

object GlobalWindowFunction {

  val globalWindow = Array(Window(Instant.ofEpochMilli(Time.MIN_TIME_MILLIS),
    Instant.ofEpochMilli(Time.MAX_TIME_MILLIS)))
}

/**
 * All elements are assigned to the same global window for calculation.
 */
case class GlobalWindowFunction() extends NonMergingWindowFunction {

  override def apply[T](context: WindowFunction.Context[T]): Array[Window] = {
    GlobalWindowFunction.globalWindow
  }
}

/**
 * Elements are assigned to non-merging sliding windows for calculation.
 *
 * @param size window size
 * @param step window step to slide forward
 */
case class SlidingWindowFunction(size: Duration, step: Duration)
  extends NonMergingWindowFunction {

  def this(size: Duration) = {
    this(size, size)
  }

  override def apply[T](context: WindowFunction.Context[T]): Array[Window] = {
    val timestamp = context.timestamp
    val sizeMillis = size.toMillis
    val stepMillis = step.toMillis
    val timeMillis = timestamp.toEpochMilli
    val windows = ArrayBuffer.empty[Window]
    var start = lastStartFor(timeMillis, stepMillis)
    windows += Window.ofEpochMilli(start, start + sizeMillis)
    start -= stepMillis
    while (start >= timeMillis) {
      windows += Window.ofEpochMilli(start, start + sizeMillis)
      start -= stepMillis
    }
    windows.toArray
  }

  private def lastStartFor(timestamp: MilliSeconds, windowStep: Long): MilliSeconds = {
    timestamp - (timestamp + windowStep) % windowStep
  }
}

/**
 * Elements are assigned to merging windows for calculation. Windows are merged
 * if their distance is within the defined gap.
 *
 * @param gap session gap
 */
case class SessionWindowFunction(gap: Duration) extends WindowFunction {

  override def apply[T](context: WindowFunction.Context[T]): Array[Window] = {
    Array(Window(context.timestamp, context.timestamp.plus(gap)))
  }

  override def isNonMerging: Boolean = false

}