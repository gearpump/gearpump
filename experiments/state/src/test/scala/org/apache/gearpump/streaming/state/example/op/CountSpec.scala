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
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class CountSpec extends PropSpec with PropertyChecks with Matchers {

  val countGen = Gen.chooseNum[Long](1, 1000)
  property("Count init should be 0") {
    val countOp = new Count
    countOp.init shouldBe 0L
  }

  property("Count update should up count by 1") {
    val msgGen = Gen.alphaStr.map(Message(_))
    forAll(msgGen, countGen) { (msg: Message, count: Long) =>
      val countOp = new Count
      countOp.update(msg, count) shouldBe count + 1
    }
  }

  property("Count should serialize and deserialize any long") {
    forAll(countGen) { (count: Long) =>
      val countOp = new Count
      countOp.deserialize(countOp.serialize(count)) shouldBe count

    }
  }
}
