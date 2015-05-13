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

package gearpump.streaming.kafka.lib

import gearpump.Message
import gearpump.TimeStamp
import org.scalacheck.Gen
import org.scalatest.{PropSpec, Matchers}
import org.scalatest.prop.PropertyChecks

class KafkaFilterSpec extends PropSpec with PropertyChecks with Matchers {

  property("KafkaFilter should filter message against give timestamp") {
    val timestampGen = Gen.chooseNum[Long](0L, 1000L)
    val messageGen = for {
      msg <- Gen.alphaStr
      time <- timestampGen
    } yield Message(msg, time)

    val filter = new KafkaFilter()

    forAll(timestampGen, messageGen) {
      (predicate: TimeStamp, message: Message) =>
        if (message.timestamp >= predicate) {
          filter.filter(message, predicate) shouldBe Some(message)
        } else {
          filter.filter(message, predicate) shouldBe None
        }

        filter.filter(null, predicate) shouldBe None
    }
  }
}
