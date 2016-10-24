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

package org.apache.gearpump.streaming.partitioner

import org.apache.gearpump.Message

import scala.util.Random

/**
 * The idea of ShuffleGroupingPartitioner is derived from Storm.
 * Messages are randomly distributed across the downstream's tasks in a way such that
 * each task is guaranteed to get an equal number of messages.
 */
class ShuffleGroupingPartitioner extends UnicastPartitioner {
  private val random = new Random
  private var index = -1
  private var partitions = List.empty[Int]
  override def getPartition(msg: Message, partitionNum: Int, currentPartitionId: Int): Int = {
    index += 1
    if (partitions.isEmpty) {
      partitions = 0.until(partitionNum).toList
      partitions = random.shuffle(partitions)
    } else if (index >= partitionNum) {
      index = 0
      partitions = random.shuffle(partitions)
    }
    partitions(index)
  }
}
