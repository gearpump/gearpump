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

import org.apache.gearpump.partitioner.{PartitionerDescription, Partitioner}
import org.apache.gearpump.streaming.{ProcessorDescription, DAG}

/**
 * Each processor can have multiple downstream subscribers.
 *
 * For example: When processor A subscribe to processor B, then the output of B will be
 * pushed to processor A.
 *
 * @param processorId subscriber processor Id
 * @param partitioner subscriber partitioner
 * @param processor subscriber processor definition
 */
case class Subscriber(processorId: Int, partitioner: PartitionerDescription, processor: ProcessorDescription)

object Subscriber {

  /**
   *
   * Subscription of processorId.
   *
   * The topology information is retrived from dag
   *
   * @param processorId
   * @param dag
   * @return
   */
  def of(processorId: Int, dag: DAG): List[Subscriber] = {
    val edges = dag.graph.outgoingEdgesOf(processorId)

    edges.foldLeft(List.empty[Subscriber]) { (list, nodeEdgeNode) =>
      val (_, partitioner, downstreamProcessorId) = nodeEdgeNode
      val downstreamProcessor = dag.processors(downstreamProcessorId)
      list :+ Subscriber(downstreamProcessorId, partitioner, downstreamProcessor)
    }
  }
}

case class Subscribe(subscriberId: Int, subscriber: Subscriber)

case class UnSubscribe(subscriberId: Int)
