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
package io.gearpump.streaming.dsl.window.impl

import com.gs.collections.api.block.predicate.Predicate
import com.gs.collections.api.block.procedure.Procedure
import com.gs.collections.impl.list.mutable.FastList
import com.gs.collections.impl.map.sorted.mutable.TreeSortedMap
import io.gearpump.Message
import io.gearpump.streaming.dsl.plan.functions.FunctionRunner
import io.gearpump.streaming.dsl.window.api.{Discarding, Windows}
import io.gearpump.streaming.dsl.window.api.WindowFunction.Context
import io.gearpump.streaming.source.Watermark
import io.gearpump.streaming.task.TaskUtil
import java.time.Instant
import scala.collection.mutable.ArrayBuffer

/**
 * Inputs for [[StreamingOperator]].
 */
case class TimestampedValue[T](value: T, timestamp: Instant) {

  def this(msg: Message) = {
    this(msg.value.asInstanceOf[T], msg.timestamp)
  }

  def toMessage: Message = Message(value, timestamp)
}

/**
 * Outputs triggered by [[StreamingOperator]]
 */
case class TriggeredOutputs[T](outputs: TraversableOnce[TimestampedValue[T]],
    watermark: Instant)


trait StreamingOperator[IN, OUT] extends java.io.Serializable {

  def setup(): Unit = {}

  def foreach(tv: TimestampedValue[IN]): Unit

  def flatMap(
      tv: TimestampedValue[IN]): TraversableOnce[TimestampedValue[OUT]] = {
    foreach(tv)
    None
  }

  def trigger(time: Instant): TriggeredOutputs[OUT]

  def teardown(): Unit = {}
}

/**
 * A composite WindowRunner that first executes its left child and feeds results
 * into result child.
 */
case class AndThenOperator[IN, MIDDLE, OUT](left: StreamingOperator[IN, MIDDLE],
    right: StreamingOperator[MIDDLE, OUT]) extends StreamingOperator[IN, OUT] {

  override def setup(): Unit = {
    left.setup()
    right.setup()
  }

  override def foreach(
      tv: TimestampedValue[IN]): Unit = {
    left.flatMap(tv).foreach(right.flatMap)
  }

  override def flatMap(
      tv: TimestampedValue[IN]): TraversableOnce[TimestampedValue[OUT]] = {
    left.flatMap(tv).flatMap(right.flatMap)
  }

  override def trigger(time: Instant): TriggeredOutputs[OUT] = {
    val lOutputs = left.trigger(time)
    lOutputs.outputs.foreach(right.foreach)
    right.trigger(lOutputs.watermark)
  }

  override def teardown(): Unit = {
    left.teardown()
    right.teardown()
  }
}

/**
 * @param runner FlatMapper or chained FlatMappers
 */
class FlatMapOperator[IN, OUT](runner: FunctionRunner[IN, OUT])
  extends StreamingOperator[IN, OUT] {

  override def setup(): Unit = {
    runner.setup()
  }

  override def foreach(tv: TimestampedValue[IN]): Unit = {
    throw new UnsupportedOperationException("foreach should not be invoked on FlatMapOperator; " +
      "please use flatMap instead")
  }

  override def flatMap(
      tv: TimestampedValue[IN]): TraversableOnce[TimestampedValue[OUT]] = {
    runner.process(tv.value)
      .map(TimestampedValue(_, tv.timestamp))
  }

  override def trigger(time: Instant): TriggeredOutputs[OUT] = {
    TriggeredOutputs(None, time)
  }

  override def teardown(): Unit = {
    runner.teardown()
  }
}

/**
 * This is responsible for executing window calculation.
 *   1. Groups elements into windows as defined by window function
 *   2. Applies window calculation to each group
 *   3. Emits results on triggering
 */
class WindowOperator[IN, OUT](
    windows: Windows,
    runner: FunctionRunner[IN, OUT])
  extends StreamingOperator[IN, OUT] {

  private val windowFn = windows.windowFn
  private val windowInputs = new TreeSortedMap[Window, FastList[TimestampedValue[IN]]]
  private var isSetup = false
  private var watermark = Watermark.MIN

  override def foreach(
      tv: TimestampedValue[IN]): Unit = {
    val wins = windowFn(new Context[IN] {
      override def element: IN = tv.value

      override def timestamp: Instant = tv.timestamp
    })
    wins.foreach { win =>
      if (windowFn.isNonMerging) {
        if (!windowInputs.containsKey(win)) {
          val inputs = new FastList[TimestampedValue[IN]]
          windowInputs.put(win, inputs)
        }
        windowInputs.get(win).add(tv)
      } else {
        merge(windowInputs, win, tv)
      }
    }

    def merge(
        winIns: TreeSortedMap[Window, FastList[TimestampedValue[IN]]],
        win: Window, tv: TimestampedValue[IN]): Unit = {
      val intersected = winIns.keySet.select(new Predicate[Window] {
        override def accept(each: Window): Boolean = {
          win.intersects(each)
        }
      })
      var mergedWin = win
      val mergedInputs = FastList.newListWith(tv)
      intersected.forEach(new Procedure[Window] {
        override def value(each: Window): Unit = {
          mergedWin = mergedWin.span(each)
          mergedInputs.addAll(winIns.remove(each))
        }
      })
      winIns.put(mergedWin, mergedInputs)
    }
  }

  override def trigger(time: Instant): TriggeredOutputs[OUT] = {
    @annotation.tailrec
    def onTrigger(
        outputs: ArrayBuffer[TimestampedValue[OUT]],
        wmk: Instant): TriggeredOutputs[OUT] = {
      if (windowInputs.notEmpty()) {
        val firstWin = windowInputs.firstKey
        if (!time.isBefore(firstWin.endTime)) {
          val inputs = windowInputs.remove(firstWin)
          if (!isSetup) {
            runner.setup()
            isSetup = true
          }
          inputs.forEach(new Procedure[TimestampedValue[IN]] {
            override def value(tv: TimestampedValue[IN]): Unit = {
              runner.process(tv.value).foreach {
                out: OUT => outputs += TimestampedValue(out, tv.timestamp)
              }
            }
          })
          runner.finish().foreach {
            out: OUT =>
              outputs += TimestampedValue(out, firstWin.endTime.minusMillis(1))
          }
          val newWmk = TaskUtil.max(wmk, firstWin.endTime)
          if (windows.accumulationMode == Discarding) {
            runner.teardown()
            // discarding, setup need to be called for each window
            isSetup = false
          }
          onTrigger(outputs, newWmk)
        } else {
          // The output watermark is the minimum of end of last triggered window
          // and start of first un-triggered window
          TriggeredOutputs(outputs, TaskUtil.min(wmk, firstWin.startTime))
        }
      } else {
        // All windows have been triggered.
        if (time == Watermark.MAX) {
          // This means there will be no more inputs
          // so it's safe to advance to the maximum watermark.
          TriggeredOutputs(outputs, Watermark.MAX)
        } else {
          TriggeredOutputs(outputs, wmk)
        }
      }
    }

    val triggeredOutputs = onTrigger(ArrayBuffer.empty[TimestampedValue[OUT]], watermark)
    watermark = TaskUtil.max(watermark, triggeredOutputs.watermark)
    TriggeredOutputs(triggeredOutputs.outputs, watermark)
  }

  override def teardown(): Unit = {
    runner.teardown()
  }
}
