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

import com.github.ghik.silencer.silent
import io.gearpump.Time.MilliSeconds
import io.gearpump.streaming.MockUtil
import io.gearpump.streaming.state.api.{Group, Serializer}
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.PropertyChecks
import scala.collection.immutable.TreeMap
import scala.util.Success

@silent // dead code following this construct
class WindowStateSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val longGen = Gen.chooseNum[Long](100L, 10000L)

  val intervalGen = for {
    st <- longGen
    et <- Gen.chooseNum[Long](st + 1, 100000L)
  } yield Interval(st, et)

  property("WindowState init should recover checkpointed state") {
    forAll(intervalGen) {
      (interval: Interval) =>
        val window = mock[Window]
        val taskContext = MockUtil.mockTaskContext
        val group = mock[Group[AnyRef]]
        val serializer = mock[Serializer[TreeMap[Interval, AnyRef]]]

        val timestamp = interval.startTime
        val zero = mock[AnyRef]
        val bytes = Array.empty[Byte]
        val data = mock[AnyRef]
        val checkpoint = TreeMap(interval -> data)
        when(group.zero).thenReturn(zero, zero)
        when(group.plus(zero, data)).thenReturn(data, Nil: _*)
        when(group.plus(data, zero)).thenReturn(data, Nil: _*)
        when(group.plus(zero, zero)).thenReturn(zero, Nil: _*)
        when(serializer.deserialize(bytes)).thenReturn(Success(checkpoint))

        val state = new WindowState[AnyRef](group, serializer, taskContext, window)
        state.left shouldBe zero
        state.right shouldBe zero
        state.get shouldBe Some(zero)

        state.recover(timestamp, bytes)

        state.left shouldBe data
        state.right shouldBe zero
        state.get shouldBe Some(data)
        state.getIntervalStates(interval.startTime, interval.endTime) shouldBe checkpoint
    }
  }

  property("WindowState checkpoints") {
    forAll(longGen) { (checkpointTime: MilliSeconds) =>
      val window = mock[Window]
      val taskContext = MockUtil.mockTaskContext
      val group = mock[Group[AnyRef]]
      val serializer = mock[Serializer[TreeMap[Interval, AnyRef]]]

      val zero = mock[AnyRef]
      val left = mock[AnyRef]
      val right = mock[AnyRef]
      val plus = mock[AnyRef]

      when(group.zero).thenReturn(zero, zero)
      when(group.plus(zero, zero)).thenReturn(zero, Nil: _*)
      val state = new WindowState[AnyRef](group, serializer, taskContext, window)
      state.left shouldBe zero
      state.right shouldBe zero
      state.get shouldBe Some(zero)

      val start = checkpointTime - 1
      val end = checkpointTime + 1
      val size = end - start
      val step = 1L

      when(window.range).thenReturn((start, end))
      when(window.windowSize).thenReturn(size)
      when(window.windowStep).thenReturn(step)
      when(group.zero).thenReturn(zero, zero)
      when(group.plus(zero, left)).thenReturn(left, Nil: _*)
      when(group.plus(zero, right)).thenReturn(right, Nil: _*)
      when(group.plus(left, right)).thenReturn(plus, Nil: _*)

      state.left = left
      state.updateIntervalStates(start, left, checkpointTime)
      state.right = right
      state.updateIntervalStates(checkpointTime, right, checkpointTime)

      state.setNextCheckpointTime(checkpointTime)
      state.checkpoint()

      state.left shouldBe plus
      state.right shouldBe zero
      verify(serializer).serialize(TreeMap(Interval(start, checkpointTime) -> left))
    }
  }

  property("WindowState updates state") {
    forAll(longGen) { (checkpointTime: MilliSeconds) =>
      val window = mock[Window]
      val taskContext = MockUtil.mockTaskContext
      val group = mock[Group[AnyRef]]
      val serializer = mock[Serializer[TreeMap[Interval, AnyRef]]]

      val zero = mock[AnyRef]
      val left = mock[AnyRef]
      val right = mock[AnyRef]
      val plus = mock[AnyRef]

      when(group.zero).thenReturn(zero, zero)
      val state = new WindowState[AnyRef](group, serializer, taskContext, window)

      val start = checkpointTime - 1
      val end = checkpointTime + 1
      val size = end - start
      val step = 1L

      when(window.range).thenReturn((start, end))
      when(window.windowSize).thenReturn(size)
      when(window.windowStep).thenReturn(step)
      when(window.shouldSlide).thenReturn(false)
      when(group.plus(zero, left)).thenReturn(left, left)
      when(group.plus(left, zero)).thenReturn(left, Nil: _*)
      when(taskContext.upstreamMinClock).thenReturn(0L)

      // Time < checkpointTime
      // Update left in current window
      state.setNextCheckpointTime(checkpointTime)
      state.update(start, left)

      verify(window).update(0L)
      state.left shouldBe left
      state.right shouldBe zero
      state.get shouldBe Some(left)
      state.getIntervalStates(start, end) shouldBe TreeMap(Interval(start, checkpointTime) -> left)

      when(window.range).thenReturn((start, end))
      when(window.windowSize).thenReturn(size)
      when(window.windowStep).thenReturn(step)
      when(window.shouldSlide).thenReturn(false)
      when(group.plus(zero, right)).thenReturn(right, right)
      when(group.plus(left, right)).thenReturn(plus, Nil: _*)
      when(taskContext.upstreamMinClock).thenReturn(0L)

      // Time >= checkpointTime
      // Update right in current window
      state.setNextCheckpointTime(checkpointTime)
      state.update(checkpointTime, right)

      verify(window, times(2)).update(0L)
      state.left shouldBe left
      state.right shouldBe right
      state.get shouldBe Some(plus)
      state.getIntervalStates(start, end) shouldBe
        TreeMap(Interval(start, start + step) -> left, Interval(start + step, end) -> right)

      // Slides window forward
      when(window.range).thenReturn((start, end), (start + step, end + step))
      when(window.shouldSlide).thenReturn(true)
      when(taskContext.upstreamMinClock).thenReturn(checkpointTime)
      when(group.minus(left, left)).thenReturn(zero, Nil: _*)
      when(group.plus(zero, right)).thenReturn(right, Nil: _*)
      when(group.plus(right, right)).thenReturn(plus, Nil: _*)
      when(group.plus(zero, plus)).thenReturn(plus, Nil: _*)

      state.setNextCheckpointTime(checkpointTime)
      state.update(end, right)

      verify(window).slideOneStep()
      verify(window).update(checkpointTime)
      state.left shouldBe zero
      state.right shouldBe plus
      state.get shouldBe Some(plus)
      state.getIntervalStates(start, end + step) shouldBe
        TreeMap(
          Interval(start, start + step) -> left,
          Interval(start + step, end) -> right,
          Interval(end, end + step) -> right)
    }
  }

  property("WindowState gets interval for timestamp") {
    forAll(longGen, longGen, longGen, longGen) {
      (timestamp: MilliSeconds, checkpointTime: MilliSeconds, windowSize: Long, windowStep: Long) =>
        val windowManager = new Window(windowSize, windowStep)
        val taskContext = MockUtil.mockTaskContext
        val group = mock[Group[AnyRef]]
        val serializer = mock[Serializer[TreeMap[Interval, AnyRef]]]

        val zero = mock[AnyRef]
        when(group.zero).thenReturn(zero, zero)
        val state = new WindowState[AnyRef](group, serializer, taskContext, windowManager)

        val interval = state.getInterval(timestamp, checkpointTime)
        intervalSpec(interval, timestamp, checkpointTime, windowSize, windowStep)

        val nextTimeStamp = interval.endTime
        val nextInterval = state.getInterval(nextTimeStamp, checkpointTime)
        intervalSpec(nextInterval, nextTimeStamp, checkpointTime, windowSize, windowStep)

        interval.endTime shouldBe nextInterval.startTime
    }

    def intervalSpec(interval: Interval, timestamp: MilliSeconds,
        checkpointTime: MilliSeconds, windowSize: Long, windowStep: Long): Unit = {
      interval.startTime should be <= interval.endTime
      timestamp / windowStep * windowStep should (be <= interval.startTime)
      (timestamp - windowSize) / windowStep * windowStep should (be <= interval.startTime)
      (timestamp / windowStep + 1) * windowStep should (be >= interval.endTime)
      ((timestamp - windowSize) / windowStep + 1) * windowStep + windowSize should
        (be >= interval.endTime)
      checkpointTime should (be <= interval.startTime or be >= interval.endTime)
    }
  }
}
