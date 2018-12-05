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

/** Used by storm module to broadcast message to all downstream tasks  */
class BroadcastPartitioner extends MulticastPartitioner {
  private var lastPartitionNum = -1
  private var partitions = Array.empty[Int]

  override def getPartitions(msg: Message, partitionNum: Int,
      currentPartitionId: Int): Array[Int] = {
    if (partitionNum != lastPartitionNum) {
      partitions = (0 until partitionNum).toArray
      lastPartitionNum = partitionNum
    }
    partitions
  }
}
