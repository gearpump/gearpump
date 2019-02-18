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

package io.gearpump.streaming.task

import io.gearpump.streaming.{DAG, LifeTime}
import io.gearpump.streaming.partitioner.PartitionerDescription

/**
 * Each processor can have multiple downstream subscribers.
 *
 * For example: When processor A subscribe to processor B, then the output of B will be
 * pushed to processor A.
 *
 * @param processorId subscriber processor Id
 * @param partitionerDescription subscriber partitioner
 */
case class Subscriber(processorId: Int, partitionerDescription: PartitionerDescription,
    parallelism: Int, lifeTime: LifeTime)

object Subscriber {

  /**
   *
   * List subscriptions of a processor.
   * The topology information is retrieved from dag
   *
   * @param processorId   the processor to list
   * @param dag     the DAG
   * @return   the subscribers of this processor
   */
  def of(processorId: Int, dag: DAG): List[Subscriber] = {
    val edges = dag.graph.outgoingEdgesOf(processorId)

    edges.foldLeft(List.empty[Subscriber]) { (list, nodeEdgeNode) =>
      val (_, partitioner, downstreamProcessorId) = nodeEdgeNode
      val downstreamProcessor = dag.processors(downstreamProcessorId)
      list :+ Subscriber(downstreamProcessorId, partitioner,
        downstreamProcessor.parallelism, downstreamProcessor.life)
    }
  }
}

