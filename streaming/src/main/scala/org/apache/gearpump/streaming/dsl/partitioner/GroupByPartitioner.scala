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

package org.apache.gearpump.streaming.dsl.partitioner

import org.apache.gearpump.Message
import org.apache.gearpump.streaming.partitioner.UnicastPartitioner
import org.apache.gearpump.streaming.dsl.window.api.GroupByFn

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
 * @param fn First apply message with groupBy function, then pick the hashCode of the output
 *   to do the partitioning. You must define hashCode() for output type of groupBy function.
 */
class GroupByPartitioner[T, Group](fn: GroupByFn[T, Group])
  extends UnicastPartitioner {
  override def getPartition(message: Message, partitionNum: Int, currentPartitionId: Int): Int = {
    val hashCode = fn.groupBy(message).hashCode()
    (hashCode & Integer.MAX_VALUE) % partitionNum
  }
}

