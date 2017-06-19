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
package org.apache.gearpump.streaming.dsl.window.impl

import java.time.Instant

import com.gs.collections.api.block.predicate.Predicate
import com.gs.collections.api.block.procedure.Procedure
import com.gs.collections.impl.list.mutable.FastList
import com.gs.collections.impl.map.sorted.mutable.TreeSortedMap
import org.apache.gearpump.streaming.dsl.plan.functions.FunctionRunner
import org.apache.gearpump.streaming.dsl.window.api.WindowFunction.Context
import org.apache.gearpump.streaming.dsl.window.api.{Discarding, Windows}
import org.apache.gearpump.streaming.source.Watermark
import org.apache.gearpump.streaming.task.TaskUtil

import scala.collection.mutable.ArrayBuffer

case class TimestampedValue[T](value: T, timestamp: Instant)

case class TriggeredOutputs[T](outputs: TraversableOnce[TimestampedValue[T]],
    watermark: Instant)

trait WindowRunner[IN, OUT] extends java.io.Serializable {

  def process(timestampedValue: TimestampedValue[IN]): Unit

  def trigger(time: Instant): TriggeredOutputs[OUT]
}

case class AndThen[IN, MIDDLE, OUT](left: WindowRunner[IN, MIDDLE],
    right: WindowRunner[MIDDLE, OUT]) extends WindowRunner[IN, OUT] {

  override def process(timestampedValue: TimestampedValue[IN]): Unit = {
    left.process(timestampedValue)
  }

  override def trigger(time: Instant): TriggeredOutputs[OUT] = {
    val lOutputs = left.trigger(time)
    lOutputs.outputs.foreach(right.process)
    right.trigger(lOutputs.watermark)
  }
}

class DefaultWindowRunner[IN, OUT](
    windows: Windows,
    fnRunner: FunctionRunner[IN, OUT])
  extends WindowRunner[IN, OUT] {

  private val windowFn = windows.windowFn
  private val windowInputs = new TreeSortedMap[Window, FastList[TimestampedValue[IN]]]
  private var setup = false
  private var watermark = Watermark.MIN

  override def process(timestampedValue: TimestampedValue[IN]): Unit = {
    val wins = windowFn(new Context[IN] {
      override def element: IN = timestampedValue.value

      override def timestamp: Instant = timestampedValue.timestamp
    })
    wins.foreach { win =>
      if (windowFn.isNonMerging) {
        if (!windowInputs.containsKey(win)) {
          val inputs = new FastList[TimestampedValue[IN]]
          windowInputs.put(win, inputs)
        }
        windowInputs.get(win).add(timestampedValue)
      } else {
        merge(windowInputs, win, timestampedValue)
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
          if (!setup) {
            fnRunner.setup()
            setup = true
          }
          inputs.forEach(new Procedure[TimestampedValue[IN]] {
            override def value(tv: TimestampedValue[IN]): Unit = {
              fnRunner.process(tv.value).foreach {
                out: OUT => outputs += TimestampedValue(out, tv.timestamp)
              }
            }
          })
          fnRunner.finish().foreach {
            out: OUT =>
              outputs += TimestampedValue(out, firstWin.endTime.minusMillis(1))
          }
          val newWmk = TaskUtil.max(wmk, firstWin.endTime)
          if (windows.accumulationMode == Discarding) {
            fnRunner.teardown()
            // discarding, setup need to be called for each window
            setup = false
          }
          onTrigger(outputs, newWmk)
        } else {
          // minimum of end of last triggered window and start of first un-triggered window
          TriggeredOutputs(outputs, TaskUtil.min(wmk, firstWin.startTime))
        }
      } else {
        if (time == Watermark.MAX) {
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
}
