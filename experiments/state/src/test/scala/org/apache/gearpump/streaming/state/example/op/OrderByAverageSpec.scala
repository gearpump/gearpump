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

import org.apache.gearpump.{TimeStamp, Message}
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

import scala.collection.immutable.TreeMap

class OrderByAverageSpec extends PropSpec with PropertyChecks with Matchers {
  property("OrderByAverage init should be empty TreeMap") {
    val orderByOp = new OrderByAverage
    orderByOp.init shouldBe TreeMap.empty[Double, TimeStamp]
  }

  property("OrderByAverage update should add to existing TreeMap") {
    val timeGen = Gen.chooseNum[Long](1, 1000)
    val sumGen = Gen.chooseNum[Long](1000, 10000)
    val countGen = Gen.chooseNum[Long](1, 1000)
    forAll(timeGen, sumGen, countGen) { (timestamp: TimeStamp, sum: Long, count: Long) =>
      val orderByOp = new OrderByAverage
      val msg = Message(timestamp -> (sum, count), timestamp)
      orderByOp.update(msg, TreeMap.empty[Double, TimeStamp]) shouldBe TreeMap((sum * 1.0 / count) -> timestamp)
    }
  }

  property("OrderByAverage should serialize and deserialize any TreeMap") {
    val tupleGen = Gen.chooseNum[Long](1, 1000).map(l => l * 1.0 -> l)
    val treeMapGen = Gen.listOf[(Double, TimeStamp)](tupleGen)
      .map(_.foldLeft(TreeMap.empty[Double, TimeStamp])((m, t) => m + t))
    forAll(treeMapGen) { (map: TreeMap[Double, TimeStamp]) =>
      val orderByOp = new OrderByAverage
      orderByOp.deserialize(orderByOp.serialize(map)) shouldBe map

    }
  }
}
