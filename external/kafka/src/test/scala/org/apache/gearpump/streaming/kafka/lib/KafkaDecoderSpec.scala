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

package org.apache.gearpump.streaming.kafka.lib

import java.nio.ByteBuffer

import com.twitter.bijection.Injection
import org.scalacheck.Gen
import org.scalatest.{PropSpec, Matchers}
import org.scalatest.prop.PropertyChecks

class KafkaDecoderSpec extends PropSpec with PropertyChecks with Matchers {
  property("KafkaDecoder should get Message from bytes") {
    val decoder = new KafkaDecoder()
    forAll(Gen.alphaStr) { (s: String) =>
      val bytes = Injection[String, Array[Byte]](s)
      decoder.fromBytes(bytes).msg shouldBe s
    }
  }

  property("KafkaDecoder should throw exception on invalid bytes") {
    val decoder = new KafkaDecoder()
    val invalidBytes: Array[Byte] = null
    intercept[RuntimeException] {
      decoder.fromBytes(invalidBytes)
    }
  }

}
