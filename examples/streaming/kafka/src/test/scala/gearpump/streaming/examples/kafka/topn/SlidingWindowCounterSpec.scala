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

package gearpump.streaming.examples.kafka.topn

import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class SlidingWindowCounterSpec extends PropSpec with PropertyChecks with Matchers {

  val objs = Array("a", "e", "i", "o", "u")
  val winLenGen = Gen.choose(5, 10)
  val objCountsGen = Gen.sized { size =>
    Gen.containerOfN[Array, Array[Long]](objs.size,
      Gen.containerOfN[Array, Long](size, Gen.choose(1, 100)))
  }

  property("SlidingWindowCounter should count objects in the configured window") {
    forAll(winLenGen, objCountsGen) { (n: Int, objCounts: Array[Array[Long]]) =>
      val counter = new SlidingWindowCounter[String](n)
      0.until(objCounts.head.size) foreach { iter =>
        0.until(objs.size) foreach { index =>
          0.until(objCounts(index)(iter).asInstanceOf[Int]) foreach { _ =>
            counter.incrementCount(objs(index))
          }
        }
        val counts = counter.getCountsThenAdvanceWindow
        val expCounts = 0.until(objs.size).map { index =>
          objs(index) -> objCounts(index).take(iter + 1).drop(iter + 1 - n).sum
        }.toMap
        counts should contain theSameElementsAs expCounts
      }
    }
  }
}
