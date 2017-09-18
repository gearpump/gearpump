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

package org.apache.gearpump.streaming

import org.apache.gearpump.streaming.partitioner.PartitionerDescription
import org.apache.gearpump.streaming.task.TaskId
import org.apache.gearpump.util.Graph

/**
 * DAG is wrapper for [[org.apache.gearpump.util.Graph]] for streaming applications.
 */
case class DAG(version: Int, processors : Map[ProcessorId, ProcessorDescription],
    graph : Graph[ProcessorId, PartitionerDescription])
  extends Serializable {

  def isEmpty: Boolean = {
    processors.isEmpty
  }

  def taskCount: Int = {
    processors.foldLeft(0) { (count, task) =>
      count + task._2.parallelism
    }
  }

  def tasks: List[TaskId] = {
    processors.flatMap { pair =>
      val (processorId, processor) = pair
      (0 until processor.parallelism).map(TaskId(processorId, _))
    }.toList
  }
}

object DAG {
  def apply(graph: Graph[ProcessorDescription, PartitionerDescription], version: Int = 0): DAG = {
    val processors = graph.getVertices.map { processorDescription =>
      (processorDescription.id, processorDescription)
    }.toMap
    val dag = graph.mapVertex { processor =>
      processor.id
    }
    new DAG(version, processors, dag)
  }

  def empty: DAG = apply(Graph.empty[ProcessorDescription, PartitionerDescription])
}