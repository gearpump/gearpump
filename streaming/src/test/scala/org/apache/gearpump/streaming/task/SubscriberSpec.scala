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
package org.apache.gearpump.streaming.task

import org.apache.gearpump.partitioner.{PartitionerDescription, HashPartitioner}
import org.apache.gearpump.streaming.task.SubscriberSpec.TestTask

import org.apache.gearpump.streaming.{ProcessorDescription, DAG}
import org.apache.gearpump.util.Graph
import org.scalatest.{FlatSpec, WordSpec, Matchers, FlatSpecLike}
import org.apache.gearpump.util.Graph._

class SubscriberSpec  extends FlatSpec with Matchers {
  "Subscriber.of" should "return all subscriber for a processor" in {

    val sourceProcessorId = 0
    val task1 = ProcessorDescription(id = sourceProcessorId, classOf[TestTask].getName, 1)
    val task2 = ProcessorDescription(id = 1, classOf[TestTask].getName, 1)
    val task3 = ProcessorDescription(id = 2, classOf[TestTask].getName, 1)
    val partitioner = new HashPartitioner()
    val dag: DAG = Graph(task1 ~ partitioner ~> task2, task1 ~ partitioner ~> task3, task2 ~ partitioner ~> task3)


    val subscribers = Subscriber.of(sourceProcessorId, dag)
    assert(subscribers.size == 2)

    assert(subscribers.toSet ==
      Set(Subscriber(1, PartitionerDescription(partitioner), task2), Subscriber(2, PartitionerDescription(partitioner), task3)))
  }
}

object SubscriberSpec {
  class TestTask
}
