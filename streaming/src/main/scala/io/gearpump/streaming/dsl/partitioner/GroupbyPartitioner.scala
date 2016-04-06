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

package io.gearpump.streaming.dsl.partitioner

import io.gearpump.Message
import io.gearpump.partitioner.UnicastPartitioner

/**
 * Partition messages by applying group by function first.
 *
 * For example:
 * {{{
 * case class People(name: String, gender: String)
 *
 * object Test{
 *
 *   val groupBy: (People => String) = people => people.gender
 *   val partitioner = GroupByPartitioner(groupBy)
 * }
 * }}}
 *
 * @param groupBy First apply message with groupBy function, then pick the hashCode of the output
 *   to do the partitioning. You must define hashCode() for output type of groupBy function.
 */
class GroupByPartitioner[T, GROUP](groupBy: T => GROUP = null) extends UnicastPartitioner {
  override def getPartition(msg: Message, partitionNum: Int, currentPartitionId: Int): Int = {
    val hashCode = groupBy(msg.msg.asInstanceOf[T]).hashCode()
    (hashCode & Integer.MAX_VALUE) % partitionNum
  }
}