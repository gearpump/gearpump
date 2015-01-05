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

package org.apache.gearpump.streaming

import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.util.Graph

import scala.collection.JavaConversions._

object DAG {

  def apply (graph : Graph[TaskDescription, Partitioner]) : DAG = {
    val topologicalOrderIterator = graph.topologicalOrderIterator

    val outputGraph = Graph.empty[Int, Partitioner]
    val (_, tasks) = topologicalOrderIterator.foldLeft((0, Map.empty[Int, TaskDescription])) { (first, task) =>
      val (taskId, tasks) = first
      outputGraph.addVertex(taskId)
      (taskId + 1, tasks + (taskId -> task))
    }

    graph.edges.foreach { edge =>
      val (node1, partitioner, node2) = edge
      outputGraph.addEdge(getTaskId(tasks, node1), partitioner, getTaskId(tasks, node2))
    }

    new DAG(tasks, outputGraph)
  }

  def empty() = apply(Graph.empty)

  private def getTaskId(tasks : Map[Int, TaskDescription], node : TaskDescription) = {
    tasks.find { task =>
      val (_, taskDescription) = task
      taskDescription.equals(node)
    }.get._1
  }
}

case class DAG(tasks : Map[Int, TaskDescription], graph : Graph[Int, Partitioner]) extends Serializable {
  /**
   *
   */
  def subGraph(task : Int) = {
    var newGraph = Graph.empty[Int, Partitioner]
    graph.edgesOf(task).foreach { edge =>
      val (node1, partitioner, node2) = edge
      newGraph.addEdge(node1, partitioner, node2)
    }
    val newMap = newGraph.vertex.foldLeft(Map.empty[Int, TaskDescription]){ (map, vertex) =>
      val task = tasks.get(vertex).get

      //clean out other in-degree and out-degree tasks' data except the task parallelism
      map + (vertex -> task.copy(null, task.parallelism))
    }
    new DAG(newMap, newGraph)
  }
}
