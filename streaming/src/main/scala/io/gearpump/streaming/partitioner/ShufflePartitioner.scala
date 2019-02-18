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
import java.util.Random

/**
 * Round Robin partition the data to downstream processor tasks.
 */
class ShufflePartitioner extends UnicastPartitioner {
  private var seed = 0
  private var count = 0

  override def getPartition(msg: Message, partitionNum: Int, currentPartitionId: Int): Int = {

    if (seed == 0) {
      seed = newSeed()
    }

    val result = ((count + seed) & Integer.MAX_VALUE) % partitionNum
    count = count + 1
    result
  }

  private def newSeed(): Int = new Random().nextInt()
}
