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
import io.gearpump.streaming.state.api.{Group, MonoidState, Serializer}
import io.gearpump.streaming.state.impl.WindowState._
import io.gearpump.streaming.task.TaskContext
import io.gearpump.util.LogUtil
import org.slf4j.Logger
import scala.collection.immutable.TreeMap

/**
 * an interval is a dynamic time range that is divided by window boundary and checkpoint time
 */
case class Interval(startTime: MilliSeconds, endTime: MilliSeconds) extends Ordered[Interval] {
  override def compare(that: Interval): Int = {
    if (startTime < that.startTime) -1
    else if (startTime > that.startTime) 1
    else 0
  }
}

object WindowState {
  val LOG: Logger = LogUtil.getLogger(classOf[WindowState[_]])
}

/**
 * this is a list of states, each of which is bounded by a time window
 * state of each window doesn't affect each other
 *
 * WindowState requires a Algebird Group to be passed in
 * Group augments Monoid with a minus function which makes it
 * possible to undo the update by messages that have left the window
 */
class WindowState[T](group: Group[T],
    serializer: Serializer[TreeMap[Interval, T]],
    taskContext: TaskContext,
    window: Window) extends MonoidState[T](group) {
  /**
   * each interval has a state updated by message with timestamp in
   * [interval.startTime, interval.endTime)
   */
  private var intervalStates = TreeMap.empty[Interval, T]

  private var lastCheckpointTime = 0L

  override def recover(timestamp: MilliSeconds, bytes: Array[Byte]): Unit = {
    window.slideTo(timestamp)
    serializer.deserialize(bytes)
      .foreach { states =>
        intervalStates = states
        left = states.foldLeft(left) { case (accum, iter) =>
          group.plus(accum, iter._2)
        }
      }
  }

  override def update(timestamp: MilliSeconds, t: T): Unit = {
    val (startTime, endTime) = window.range
    if (timestamp >= startTime && timestamp < endTime) {
      updateState(timestamp, t)
    }

    updateIntervalStates(timestamp, t, checkpointTime)

    val upstreamMinClock = taskContext.upstreamMinClock
    window.update(upstreamMinClock)

    if (window.shouldSlide) {
      window.slideOneStep()

      val (newStartTime, newEndTime) = window.range
      getIntervalStates(startTime, newStartTime).foreach { case (_, st) =>
        left = group.minus(left, st)
      }
      if (checkpointTime > endTime) {
        getIntervalStates(endTime, checkpointTime).foreach { case (_, st) =>
          left = group.plus(left, st)
        }
      } else {
        getIntervalStates(endTime, newEndTime).foreach { case (_, st) =>
          right = group.plus(right, st)
        }
      }
    }
  }

  override def checkpoint(): Array[Byte] = {
    left = group.plus(left, right)
    right = group.zero

    val states = getIntervalStates(window.range._1, checkpointTime)
    lastCheckpointTime = checkpointTime
    LOG.debug(s"checkpoint time: $checkpointTime; checkpoint value: ($checkpointTime, $states)")
    serializer.serialize(states)
  }

  /**
   * Each message will update state in corresponding Interval[StartTime, endTime),
   *
   * which is decided by the message's timestamp t where
   * {{{
   * startTime = Math.max(lowerBound1, lowerBound2, checkpointTime)
   * endTime = Math.min(upperBound1, upperBound2, checkpointTime)
   * lowerBound1 = step * Nmax1 <= t
   * lowerBound2 = step * Nmax2 + size <= t
   * upperBound1 = step * Nmin1 > t
   * upperBound2 = step * Nmin2 + size > t
   * }}}
   */
  private[impl] def getInterval(timestamp: MilliSeconds, checkpointTime: MilliSeconds): Interval = {
    val windowSize = window.windowSize
    val windowStep = window.windowStep
    val lowerBound1 = timestamp / windowStep * windowStep
    val lowerBound2 =
      if (timestamp < windowSize) 0L
      else (timestamp - windowSize) / windowStep * windowStep + windowSize
    val upperBound1 = (timestamp / windowStep + 1) * windowStep
    val upperBound2 =
      if (timestamp < windowSize) windowSize
      else ((timestamp - windowSize) / windowStep + 1) * windowStep + windowSize
    val lowerBound = Math.max(lowerBound1, lowerBound2)
    val upperBound = Math.min(upperBound1, upperBound2)
    if (checkpointTime > timestamp) {
      Interval(Math.max(lowerBound, lastCheckpointTime), Math.min(upperBound, checkpointTime))
    } else {
      Interval(Math.max(lowerBound, checkpointTime), upperBound)
    }
  }

  private[impl] def updateIntervalStates(timestamp: MilliSeconds, t: T,
      checkpointTime: MilliSeconds): Unit = {
    val interval = getInterval(timestamp, checkpointTime)
    intervalStates.get(interval) match {
      case Some(st) =>
        intervalStates += interval -> group.plus(st, t)
      case None =>
        intervalStates += interval -> group.plus(group.zero, t)
    }
  }

  private[impl] def getIntervalStates(startTime: MilliSeconds, endTime: MilliSeconds)
  : TreeMap[Interval, T] = {
    intervalStates.dropWhile(_._1.endTime <= startTime).takeWhile(_._1.endTime <= endTime)
  }
}
