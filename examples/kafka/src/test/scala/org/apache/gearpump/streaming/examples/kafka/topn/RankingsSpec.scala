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

package org.apache.gearpump.streaming.examples.kafka.topn

import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class RankingsSpec extends PropSpec with PropertyChecks with Matchers {

  val objCountGen = for {
    obj <- Gen.alphaStr
    count <- Gen.choose[Long](1, 100)
  } yield (obj, count)
  val objCountList = Gen.containerOf[List, (String, Long)](objCountGen)
  val topn = Gen.choose[Int](1, 10)

  property("rankings should keep top n values") {
    forAll(topn, objCountList) { (n: Int, m: List[(String, Long)]) =>
      val rankings = new Rankings[String]
      m.foreach(r => rankings.update(r._1, r._2))
      rankings.getTopN(n) should contain theSameElementsInOrderAs m.sortBy(_._2).reverse.take(n)
    }
  }

  property("rankings should be empty after clear") {
    forAll(topn, objCountList) { (n: Int, m: List[(String, Long)]) =>
      val rankings = new Rankings[String]
      m.foreach(r => rankings.update(r._1, r._2))
      rankings.clear()
      rankings.getTopN(n) shouldBe empty
    }
  }
}
