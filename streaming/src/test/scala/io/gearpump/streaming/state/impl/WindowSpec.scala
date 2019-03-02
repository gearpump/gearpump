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
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.PropertyChecks

class WindowSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val windowSizeGen = Gen.chooseNum[Long](1L, 1000L)
  val windowStepGen = Gen.chooseNum[Long](1L, 1000L)
  val timestampGen = Gen.chooseNum[Long](0L, 1000L)
  property("Window should only slide when time passes window end") {
    forAll(timestampGen, windowSizeGen, windowStepGen) {
      (timestamp: MilliSeconds, windowSize: Long, windowStep: Long) =>
        val window = new Window(windowSize, windowStep)
        window.shouldSlide shouldBe false
        window.update(timestamp)
        window.shouldSlide shouldBe timestamp >= windowSize
    }
  }

  property("Window should slide by one or to given timestamp") {
    forAll(timestampGen, windowSizeGen, windowStepGen) {
      (timestamp: MilliSeconds, windowSize: Long, windowStep: Long) =>
        val window = new Window(windowSize, windowStep)
        window.range shouldBe(0L -> windowSize)

        window.slideOneStep()
        window.range shouldBe(windowStep -> (windowSize + windowStep))

        window.slideTo(timestamp)
        val (startTime, endTime) = window.range
        if (windowStep > windowSize) {
          timestamp should (be >= startTime and be < (startTime + windowStep))
        } else {
          timestamp should (be >= startTime and be < endTime)
        }
    }
  }
}
