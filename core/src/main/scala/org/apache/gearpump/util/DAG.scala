package org.apache.gearpump.util

import org.apache.gearpump.{Partitioner, TaskDescription}
import java.util.Random
import org.apache.gearpump.task.TaskId
import org.apache.gearpump.{Partitioner, TaskDescription}
import org.jgrapht.Graphs

import scala.collection.JavaConversions._
import org.apache.gearpump.util.Graph.{Edge}
import org.jgrapht.graph.{AbstractBaseGraph, DefaultDirectedGraph, DefaultEdge}

import scala.collection.mutable.HashMap
import scala.reflect.ClassTag
import org.jgrapht.traverse.TopologicalOrderIterator
import org.jgrapht.Graphs

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

object DAG {

  import Graph._

  def apply (graph : Graph[TaskDescription, Partitioner]) : DAG = {
    val iter = graph.topoIter

    val outputGraph = Graph.empty[Int, Partitioner]
    val (_, tasks) = iter.foldLeft((0, Map.empty[Int, TaskDescription])) { (stage, task) =>
      val (stageId, map) = stage
      outputGraph.addVertex(stageId)
      (stageId + 1, map + (stageId -> task))
    }

    graph.edges.foreach { edge =>
      val node1 ~ partitioner ~> node2 = edge
      outputGraph.addEdge(getTaskId(tasks, node1), getTaskId(tasks, node2), partitioner)
    }

    new DAG(tasks, outputGraph)
  }

  private def getTaskId(tasks : Map[Int, TaskDescription], node : TaskDescription) = {
    tasks.find { task =>
      val (_, taskDescription) = task
      taskDescription.equals(node)
    }.get._1
  }
}

class DAG(val tasks : Map[Int, TaskDescription], val graph : Graph[Int, Partitioner]) extends Serializable {
  import Graph._

  def subGraph(stageId : Int) = {
    var newGraph = Graph.empty[Int, Partitioner]
    graph.edgesOf(stageId).foreach { edge =>
      val node1 ~ partitioner ~> node2 = edge
      newGraph.addEdge(node1, node2, partitioner)
    }
    val newMap = newGraph.vertex.foldLeft(Map.empty[Int, TaskDescription]){ (map, vertex) =>
      val task = tasks.get(vertex).get
      map + (vertex -> task.copy(null, task.parallism))
    }
    new DAG(newMap, newGraph)
  }
}
