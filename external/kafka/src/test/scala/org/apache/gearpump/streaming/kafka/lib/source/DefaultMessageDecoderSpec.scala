/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package org.apache.gearpump.streaming.kafka.lib.source

import com.twitter.bijection.Injection
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class DefaultMessageDecoderSpec extends PropSpec with PropertyChecks with Matchers {
  property("DefaultMessageDecoder should keep the original bytes data in Message") {
    val decoder = new DefaultMessageDecoder()
    forAll(Gen.chooseNum[Int](0, 100), Gen.alphaStr) { (k: Int, v: String) =>
      val kbytes = Injection[Int, Array[Byte]](k)
      val vbytes = Injection[String, Array[Byte]](v)
      val timestamp = System.currentTimeMillis()
      val message = decoder.fromBytes(kbytes, vbytes)
      message.msg shouldBe vbytes
      message.timestamp should be >= timestamp
    }
  }
}

class StringMessageDecoderSpec extends PropSpec with PropertyChecks with Matchers {
  property("StringMessageDecoder should decode original bytes data into string") {
    val decoder = new StringMessageDecoder()
    forAll(Gen.alphaStr, Gen.alphaStr) { (k: String, v: String) =>
      val kbytes = Injection[String, Array[Byte]](k)
      val vbytes = Injection[String, Array[Byte]](v)
      val timestamp = System.currentTimeMillis()
      val message = decoder.fromBytes(kbytes, vbytes)
      message.msg shouldBe v
      message.timestamp should be >= timestamp
    }
  }
}
