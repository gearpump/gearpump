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

package org.apache.gearpump.streaming.transaction.checkpoint

import org.apache.gearpump.TimeStamp
import org.scalatest.{PropSpec, Matchers}
import org.scalatest.prop.PropertyChecks
import org.scalacheck.Gen

class OffsetFilterSpec extends PropSpec with PropertyChecks with Matchers {

  val predicateGen: Gen[TimeStamp] = Gen.choose(300L, 400L)
  val timeAndOffsetGen = for {
    time <- Gen.choose(0L, 1000L)
    offset <- Gen.choose(0L, 1000L)
  } yield (time, offset)
  val timeAndOffsetListGen = Gen.containerOf[List, (TimeStamp, Long)](timeAndOffsetGen) suchThat (_.size > 0)

  property("OffsetFilter should return None for empty input") {
    forAll { (time: Long) =>
      val filter = new OffsetFilter
      filter.filter(Nil, time) shouldBe None
    }
  }

  property("OffsetFilter should filter out offsets earlier than given timestamp") {
    forAll(timeAndOffsetListGen) {
      (timeAndOffsets: List[(TimeStamp, Long)]) =>
        val filter = new OffsetFilter
        val predicate = timeAndOffsets.head
        filter.filter(timeAndOffsets, predicate._1).get shouldBe predicate
    }
  }

  property("RelaxedTimeFilter should filter out offsets earlier than given relaxed timestamp") {
    forAll(timeAndOffsetListGen, Gen.choose(0L, 100L)) {
      (timeAndOffsets: List[(TimeStamp, Long)], delta: Long) =>
        val filter = new RelaxedTimeFilter(delta)
        val predicate = timeAndOffsets.head
        filter.filter(timeAndOffsets, predicate._1 + delta).get shouldBe predicate
    }
  }
}
