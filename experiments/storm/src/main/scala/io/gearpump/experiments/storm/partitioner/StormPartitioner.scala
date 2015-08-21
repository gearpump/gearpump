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
import io.gearpump.partitioner.{MulticastPartitioner, Partitioner}

/**
 * partitioner for storm applications
 * partitioning is already done in [[StormOutputCollector]] and
 * kept in [[GearpumpTuple]] as the mapping of target to partitions
 * this partitioner just gets the partitions for the target
 * @param target target storm component id
 */
class StormPartitioner(target: String) extends MulticastPartitioner {

  override def getPartitions(msg: Message, partitionNum: Int, currentPartitionId: Int): List[Int] = {
    val stormTuple = msg.msg.asInstanceOf[GearpumpTuple]
    stormTuple.targetPartitions.getOrElse(target, List(Partitioner.UNKNOWN_PARTITION_ID))
  }
}

