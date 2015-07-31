/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming.hadoop.lib.rotation

import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

import scala.concurrent.duration._

class TimeDurationRotationSpec extends PropSpec with PropertyChecks with Matchers {

  val offsetGen = Gen.chooseNum[Long](0L, 1000L)
  val durationGen = Gen.chooseNum[Int](1, 1000)

  property("TimeDurationRotation rotates on time duration") {
    forAll(offsetGen, durationGen) { (offset: Long, duration: Int) =>
      List(
        Rotation(duration seconds),
        Rotation(duration minutes),
        Rotation(duration hours),
        Rotation(duration days)).foreach {
        rotation =>
          rotation.shouldRotate shouldBe false
          rotation.mark(0L, offset)
          rotation.shouldRotate shouldBe false
          rotation.mark(rotation.maxDuration / 2, offset)
          rotation.shouldRotate shouldBe false
          rotation.mark(rotation.maxDuration, offset)
          rotation.shouldRotate shouldBe true
          rotation.rotate
          rotation.shouldRotate shouldBe false
      }

    }
  }

}
