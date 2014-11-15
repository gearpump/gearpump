/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming.examples.kafka.topn

import scala.collection.mutable.{Map => MutableMap}

object SlidingWindowCounter {
  private class SlotBasedCounter[T](numSlots: Int) {
    private val objToCounts = MutableMap.empty[T, Array[Long]]

    def incrementCount(obj: T, slot: Int): Unit = {
      val counts = objToCounts.getOrElse(obj, new Array[Long](numSlots))
      counts(slot) += 1
      objToCounts.put(obj, counts)
    }

    def getCounts: Map[T, Long] = {
      objToCounts.map(entry => (entry._1, entry._2.reduce(_ + _))).toMap
    }

    def getCount(obj: T, slot: Int): Long = {
      val counts = objToCounts.get(obj)
      if (counts.isDefined) {
        counts.get(slot)
      } else {
        0L
      }
    }

    def wipeSlot(slot: Int): Unit = {
      objToCounts.foreach(entry => entry._2(slot) = 0)
    }

    def wipeZeros: Unit = {
      objToCounts
        .filter(entry => entry._2.reduce(_ + _) == 0)
        .foreach(objToCounts - _._1)
    }
  }
}

class SlidingWindowCounter[T](windowLengthInSlots: Int) {
  import org.apache.gearpump.streaming.examples.kafka.topn.SlidingWindowCounter._

  private val objCounter: SlotBasedCounter[T] = new SlotBasedCounter[T](windowLengthInSlots)
  private var headSlot: Int = 0
  private var tailSlot: Int = slotAfter(headSlot)

  def incrementCount(obj: T): Unit = {
    objCounter.incrementCount(obj, headSlot)
  }

  def getCountsThenAdvanceWindow: Map[T, Long] = {
    val counts = objCounter.getCounts
    objCounter.wipeZeros
    objCounter.wipeSlot(tailSlot)
    advanceHead
    counts
  }

  private def advanceHead: Unit = {
    headSlot = tailSlot
    tailSlot = slotAfter(headSlot)
  }

  private def slotAfter(slot: Int): Int = {
    (slot + 1) % windowLengthInSlots
  }
}

