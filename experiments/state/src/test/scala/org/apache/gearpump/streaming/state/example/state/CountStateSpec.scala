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

import org.scalacheck.Gen
import org.scalatest.{PropSpec, Matchers}
import org.scalatest.prop.PropertyChecks

class CountStateSpec extends PropSpec with PropertyChecks with Matchers {

  property("CountState init should be 0") {
    val countState = new CountState
    countState.init shouldBe 0L
  }

  val longGen = Gen.chooseNum[Long](1L, 1000L)
  property("CountState update should add up state and data") {
    forAll(longGen, longGen) { (state: Long, data: Long) =>
      val countState = new CountState
      countState.update(state, data) shouldBe state + data
    }
  }

  property("CountState should deserialize back what has been serialized out") {
    forAll(longGen) { (state: Long) =>
      val countState = new CountState
      countState.deserialize(countState.serialize(state)) shouldBe state
    }
  }
}
