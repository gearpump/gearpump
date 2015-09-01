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

package io.gearpump.streaming.dsl

import io.gearpump.Message
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class CollectionDataSourceSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("CollectionDataSource should read from Seq") {
    val seqGen = Gen.listOf[Long](Gen.chooseNum[Long](1L, 1000L))
    val batchSizeGen = Gen.chooseNum[Int](1, 1000)
    forAll(seqGen, batchSizeGen) { (seq: Seq[Long], batchSize: Int) =>
      val source = new CollectionDataSource[String](seq.map(_.toString))
      val timeExtractor = TimeExtractor[String](t => t.toLong)
      val messageGen = (l: Long) => Message(l.toString, l)
      source.setTimeExtractor(timeExtractor)
      source.read(batchSize) shouldBe seq.take(batchSize).map(messageGen)
      if (batchSize <= seq.length) {
        source.read(batchSize) shouldBe seq.slice(batchSize, batchSize).map(messageGen)
      } else {
        source.read(batchSize) shouldBe seq.take(batchSize).map(messageGen)
      }
    }
  }
}

