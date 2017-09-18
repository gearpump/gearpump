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

package org.apache.gearpump.streaming.dsl.plan

import akka.actor.ActorSystem
import org.apache.gearpump.streaming.partitioner.{CoLocationPartitioner, GroupByPartitioner, HashPartitioner, Partitioner}
import org.apache.gearpump.streaming.Processor
import org.apache.gearpump.streaming.task.Task
import org.apache.gearpump.util.Graph

/**
 * This class is responsible for turning the high level
 * [[org.apache.gearpump.streaming.dsl.scalaapi.Stream]] DSL into low level
 * [[org.apache.gearpump.streaming.Processor]] API.
 */
class Planner {

  /**
   * This method interprets a Graph of [[Op]] and creates a Graph of
   * [[org.apache.gearpump.streaming.Processor]].
   *
   * It firstly reversely traverses the Graph from a leaf Op and merges it with
   * its downstream Op according to the following rules.
   *
   *   1. The Op has only one outgoing edge and the downstream Op has only one incoming edge
   *   2. Neither Op is [[ProcessorOp]]
   *   3. The edge is [[Direct]]
   *
   * Finally the vertices of the optimized Graph are translated to Processors
   * and the edges to Partitioners.
   */
  def plan(dag: Graph[Op, OpEdge])
    (implicit system: ActorSystem): Graph[Processor[_ <: Task], _ <: Partitioner] = {

    val graph = optimize(dag)
    graph.mapEdge { (_, edge, node2) =>
      edge match {
        case Shuffle =>
          node2 match {
            case op: GroupByOp[_, _] =>
              new GroupByPartitioner(op.groupBy)
            case _ => new HashPartitioner
          }
        case Direct =>
          // FIXME: This is never used
          new CoLocationPartitioner
      }
    }.mapVertex(_.toProcessor)
  }

  private def optimize(dag: Graph[Op, OpEdge])
    (implicit system: ActorSystem): Graph[Op, OpEdge] = {
    val graph = dag.copy
    val nodes = graph.topologicalOrderIterator.toList.reverse
    for (node <- nodes) {
      val outGoingEdges = graph.outgoingEdgesOf(node)
      for (edge <- outGoingEdges) {
        merge(graph, edge._1, edge._3)
      }
    }
    graph
  }

  private def merge(graph: Graph[Op, OpEdge], node1: Op, node2: Op)
    (implicit system: ActorSystem): Unit = {
    if (graph.outDegreeOf(node1) == 1 &&
      graph.inDegreeOf(node2) == 1 &&
      // For processor node, we don't allow it to merge with downstream operators
      !node1.isInstanceOf[ProcessorOp[_ <: Task]] &&
      !node2.isInstanceOf[ProcessorOp[_ <: Task]]) {
      val (_, edge, _) = graph.outgoingEdgesOf(node1).head
      if (edge == Direct) {
        val chainedOp = node1.chain(node2)
        graph.addVertex(chainedOp)
        for (incomingEdge <- graph.incomingEdgesOf(node1)) {
          graph.addEdge(incomingEdge._1, incomingEdge._2, chainedOp)
        }

        for (outgoingEdge <- graph.outgoingEdgesOf(node2)) {
          graph.addEdge(chainedOp, outgoingEdge._2, outgoingEdge._3)
        }

        // Remove the old vertex
        graph.removeVertex(node1)
        graph.removeVertex(node2)
      }
    }
  }
}