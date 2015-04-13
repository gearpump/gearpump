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

package org.apache.gearpump.streaming.state.example.op

import org.apache.gearpump.Message
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

class WindowCountSpec extends PropSpec with PropertyChecks with Matchers {

  val countGen = Gen.chooseNum[Long](1, 1000)
  property("WindowCount init should be (0, 0)") {
    val windowCountOp = new WindowCount
    windowCountOp.init shouldBe (0L, 0L)
  }

  property("WindowCount update should add to sum and up count by 1") {
    forAll(countGen) { (count: Long) =>
      val windowCountOp = new WindowCount
      windowCountOp.update(Message(count + ""), (count, count)) shouldBe (count + count, count + 1)
    }
  }

  property("Count should serialize and deserialize any (long, long)") {
    forAll(countGen) { (count: Long) =>
      val windowCountOp = new WindowCount
      windowCountOp.deserialize(windowCountOp.serialize((count, count))) shouldBe (count, count)

    }
  }
}
