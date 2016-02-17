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

package io.gearpump.experiments.storm.partitioner

import io.gearpump.Message
import io.gearpump.experiments.storm.topology.GearpumpTuple
import io.gearpump.experiments.storm.util.StormOutputCollector
import io.gearpump.partitioner.{Partitioner, MulticastPartitioner}

/**
 * this is a partitioner bound to a target Storm component
 * partitioning is already done in [[StormOutputCollector]] and
 * kept in "targetPartitions" of [[GearpumpTuple]]
 * the partitioner just returns the partitions of the target
 *
 * In Gearpump, a message is sent from a task to all the subscribers.
 * In Storm, however, a message is sent to one or more of the subscribers.
 * Hence, we have to do the partitioning in [[StormOutputCollector]] till the Storm way
 * is supported in Gearpump
 *
 * @param target target storm component id
 */
private[storm] class StormPartitioner(target: String) extends MulticastPartitioner {

  override def getPartitions(msg: Message, partitionNum: Int, currentPartitionId: Int): Array[Int] = {
    val stormTuple = msg.msg.asInstanceOf[GearpumpTuple]
    stormTuple.targetPartitions.getOrElse(target, Array(Partitioner.UNKNOWN_PARTITION_ID))
  }
}

