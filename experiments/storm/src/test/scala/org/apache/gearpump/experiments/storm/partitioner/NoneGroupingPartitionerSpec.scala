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

package org.apache.gearpump.experiments.storm.partitioner

import org.apache.gearpump.Message
import org.scalacheck.Gen
import org.scalatest.{PropSpec, Matchers}
import org.scalatest.prop.PropertyChecks

class NoneGroupingPartitionerSpec extends PropSpec with PropertyChecks with Matchers {

  property("NoneGroupingPartitioner should get partition in [0, partitionNum)") {
    val messageGen = Gen.alphaStr.map(Message(_))
    val partitionNumGen = Gen.chooseNum[Int](1, 1000)
    val partitioner = new NoneGroupingPartitioner
    forAll(messageGen, partitionNumGen) { (message: Message, partitionNum: Int) =>
      partitioner.getPartition(message, partitionNum, 0) should (be < partitionNum and be >= 0)
    }
  }
}
