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

package org.apache.gearpump.streaming.state.internal.api

import org.apache.gearpump.TimeStamp
import org.scalacheck.Gen
import org.scalatest.{PropSpec, Matchers}
import org.scalatest.prop.PropertyChecks

class WindowManagerSpec extends PropSpec with PropertyChecks with Matchers {

  val timestampGen = Gen.chooseNum[Long](1L, 1000L)
  val windowSizeGen = Gen.chooseNum[Long](1L, 1000L)
  val windowNumGen = Gen.chooseNum[Long](1, 10)
  val windowStrideGen = Gen.chooseNum[Long](1, 10)

  property("WindowManager syncTime should init startTime and endTime") {
    forAll(timestampGen, windowNumGen) { (timestamp: TimeStamp, windowNum: Long) =>
      val windowSize = timestamp * 2
      val windowManager = new WindowManager(windowSize, windowNum)
      windowManager.getMergedWindow shouldBe None

      windowManager.syncTime(timestamp)

      windowManager.getTime shouldBe timestamp
      windowManager.getMergedWindow shouldBe Some(CheckpointWindow(0L, windowSize * windowNum))
    }
  }

  property("WindowManager should forward if endTime is exceeded") {
    forAll(timestampGen) { (timestamp: TimeStamp) =>
      val windowSize = timestamp * 2
      val windowManager = new WindowManager(windowSize)
      windowManager.shouldForward shouldBe false

      windowManager.syncTime(timestamp)
      windowManager.syncTime(windowSize)
      windowManager.shouldForward shouldBe true
    }
  }

  property("WindowManager should forward by defined stride") {
    forAll(windowSizeGen, windowNumGen, windowStrideGen) {
      (windowSize, windowNum, windowStride) =>
        val windowManager = new WindowManager(windowSize, windowNum, windowStride)
        windowManager.syncTime(0L)
        windowManager.forward()
        windowManager.getMergedWindow shouldBe
          Some(CheckpointWindow(windowSize * windowStride, windowSize * (windowNum + windowStride)))
    }
  }

  property("WindowManager should forward to a given timestamp") {
    forAll(timestampGen, windowSizeGen, windowNumGen) {
      (timestamp, windowSize, windowNum) =>
        val windowManager = new WindowManager(windowSize, windowNum)
        windowManager.syncTime(0L)
        windowManager.forwardTo(timestamp)
        val window = windowManager.getMergedWindow.getOrElse(fail("merged window not found"))
        val startTime = window.startTime
        val endTime = window.endTime
        timestamp should (be >= startTime and be < endTime)
    }
  }

  property("WindowManager should get all managed windows") {
    forAll(windowSizeGen, windowNumGen, windowStrideGen) {
      (windowSize: Long, windowNum: Long, windowStride: Long) =>
        val windowManager = new WindowManager(windowSize, windowNum, windowStride)
        windowManager.getWindows shouldBe empty
        windowManager.syncTime(0L)
        val expected = 0L.until(windowNum).map(i => CheckpointWindow(i * windowSize, i * windowSize + windowSize)).toArray
        windowManager.getWindows shouldBe expected
    }
  }

}
