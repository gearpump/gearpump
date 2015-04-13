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

package org.apache.gearpump.streaming.state.example.state

import com.twitter.algebird.AveragedValue
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

class WindowAverageStateSpec extends PropSpec with PropertyChecks with Matchers {

  property("WindowAverageState init should be 0") {
    val windowAverageState = new WindowAverageState
    windowAverageState.init shouldBe AveragedValue(0L, 0.0)
  }

  val countGen = Gen.chooseNum[Long](1L, 1000L)
  val valueGen = Gen.chooseNum[Double](1.0, 1000.0)
  val averagedValueGen = for {
    count <- countGen
    value <- valueGen
  } yield AveragedValue(count, value)
  property("WindowAverageState update should add up two average values") {
    forAll(averagedValueGen, averagedValueGen) { (state: AveragedValue, data: AveragedValue) =>
      val windowAverageState = new WindowAverageState
      val newCount = state.count + data.count
      val newValue=
        (state.count * state.value + data.count * data.value) / (state.count + data.count)
      val result = windowAverageState.update(state, data)
      result.count shouldBe newCount
      (result.value -  newValue).abs should (be < 0.00001)
    }
  }

  property("WindowAverageState should deserialize back what has been serialized out") {
    forAll(averagedValueGen) { (state: AveragedValue) =>
      val windowAverageState = new WindowAverageState
      windowAverageState.deserialize(windowAverageState.serialize(state)) shouldBe state
    }
  }
}

