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

package org.apache.gearpump.streaming.dsl.partitioner

import org.apache.gearpump.Message
import org.apache.gearpump.streaming.dsl.partitioner.GroupByPartitionerSpec.People
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}

class GroupByPartitionerSpec  extends FlatSpec with Matchers with BeforeAndAfterAll {
  it should "use the outpout of groupBy function to do partition" in {
    val mark = People("Mark", "male")
    val tom = People("Tom", "male")
    val michelle = People("Michelle", "female")

    val partitionNum = 10
    val groupBy = new GroupByPartitioner[People, String](_.gender)
    assert(groupBy.getPartition(Message(mark), partitionNum)
      == groupBy.getPartition(Message(tom), partitionNum))

    assert(groupBy.getPartition(Message(mark), partitionNum)
      != groupBy.getPartition(Message(michelle), partitionNum))
  }
}

object GroupByPartitionerSpec {
  case class People(name: String, gender: String)
}
