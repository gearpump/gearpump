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

import org.apache.gearpump.partitioner.{CoLocationPartitioner, HashPartitioner, Partitioner}
import org.apache.gearpump.streaming.Processor
import org.apache.gearpump.streaming.dsl.op._
import org.apache.gearpump.streaming.dsl.partitioner.GroupByPartitioner
import org.apache.gearpump.streaming.task.Task
import org.apache.gearpump.util.Graph

class Planner {

  /**
   * Converts Dag of Op to Dag of TaskDescription. TaskDescription is part of the low
   * level Graph API.
   */
  def plan(dag: Graph[Op, OpEdge])(implicit system: ActorSystem)
    : Graph[Processor[_ <: Task], _ <: Partitioner] = {

    val opTranslator = new OpTranslator()

    val newDag = optimize(dag)
    newDag.mapEdge { (node1, edge, node2) =>
      edge match {
        case Shuffle =>
          node2.head match {
            case groupBy: GroupByOp[Any @unchecked, Any @unchecked] =>
              new GroupByPartitioner(groupBy.fun)
            case _ => new HashPartitioner
          }
        case Direct =>
          new CoLocationPartitioner
      }
    }.mapVertex { opChain =>
      opTranslator.translate(opChain)
    }
  }

  private def optimize(dag: Graph[Op, OpEdge]): Graph[OpChain, OpEdge] = {
    val newGraph = dag.mapVertex(op => OpChain(List(op)))

    val nodes = newGraph.topologicalOrderWithCirclesIterator.toList.reverse
    for (node <- nodes) {
      val outGoingEdges = newGraph.outgoingEdgesOf(node)
      for (edge <- outGoingEdges) {
        merge(newGraph, edge._1, edge._3)
      }
    }
    newGraph
  }

  private def merge(dag: Graph[OpChain, OpEdge], node1: OpChain, node2: OpChain)
    : Graph[OpChain, OpEdge] = {
    if (dag.outDegreeOf(node1) == 1 &&
      dag.inDegreeOf(node2) == 1 &&
      // For processor node, we don't allow it to merge with downstream operators
      !node1.head.isInstanceOf[ProcessorOp[_ <: Task]]) {
      val (_, edge, _) = dag.outgoingEdgesOf(node1).head
      if (edge == Direct) {
        val opList = OpChain(node1.ops ++ node2.ops)
        dag.addVertex(opList)
        for (incomingEdge <- dag.incomingEdgesOf(node1)) {
          dag.addEdge(incomingEdge._1, incomingEdge._2, opList)
        }

        for (outgoingEdge <- dag.outgoingEdgesOf(node2)) {
          dag.addEdge(opList, outgoingEdge._2, outgoingEdge._3)
        }

        // Remove the old vertex
        dag.removeVertex(node1)
        dag.removeVertex(node2)
      }
    }
    dag
  }
}