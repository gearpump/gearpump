/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.streaming.partitioner

import io.gearpump.Message
import org.scalatest.{FlatSpec, Matchers}

class PartitionerSpec extends FlatSpec with Matchers {
  val NUM = 10

  "HashPartitioner" should "hash same key to same slots" in {
    val partitioner = new HashPartitioner

    val data = new Array[Byte](1000)
    (new java.util.Random()).nextBytes(data)
    val msg = Message(data)

    val partition = partitioner.getPartition(msg, NUM)
    assert(partition >= 0 && partition < NUM, "Partition Id should be >= 0")

    assert(partition == partitioner.getPartition(msg, NUM), "multiple run should return" +
      "consistent result")
  }

  "ShufflePartitioner" should "hash same key randomly" in {
    val partitioner = new ShufflePartitioner

    val data = new Array[Byte](1000)
    (new java.util.Random()).nextBytes(data)
    val msg = Message(data)

    val partition = partitioner.getPartition(msg, NUM)
    assert(partition >= 0 && partition < NUM, "Partition Id should be >= 0")

    assert(partition != partitioner.getPartition(msg, NUM), "multiple run should return" +
      "consistent result")
  }

  "BroadcastPartitioner" should "return all partitions" in {
    val partitioner = new BroadcastPartitioner

    val data = new Array[Byte](1000)
    (new java.util.Random()).nextBytes(data)
    val msg = Message(data)
    val partitions = partitioner.getPartitions(msg, NUM)

    partitions should contain theSameElementsAs 0.until(NUM)
  }


  "ShuffleGroupingPartitioner" should "hash same key randomly" in {
    val partitioner = new ShuffleGroupingPartitioner

    val data = new Array[Byte](1000)
    (new java.util.Random()).nextBytes(data)
    val msg = Message(data)

    val partition = partitioner.getPartition(msg, NUM)
    assert(partition >= 0 && partition < NUM, "Partition Id should be >= 0")

    assert(partition != partitioner.getPartition(msg, NUM), "multiple run should return" +
      "consistent result")
  }
}
