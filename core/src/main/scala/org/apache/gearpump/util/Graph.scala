package org.apache.gearpump.util

import java.io.{DataOutputStream, ObjectOutputStream}
import java.util.Random
import org.apache.gearpump.examples.sol.SOLBolt
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

/**
 * Application DAG
 */

import java.io.ByteArrayOutputStream

class Graph[N, E](val graph : DefaultDirectedGraph[N, Edge[E]]) extends Serializable{
  import GraphHelper._
  import Graph._

  def addVertex(vertex : N) : Unit = {
    graph.addVertex(vertex)
  }

  def vertex : Iterator[N] = {
    graph.vertexSet().iterator()
  }

  def outDegreeOf(node : N) = {
    graph.outDegreeOf(node)
  }

  def outgoingEdgesOf(node : N) : Iterator[~>[~[N, E], N]] = {
    toEdgeIterator(graph.outgoingEdgesOf(node))
  }

  def addEdge(node1 : N, node2: N, edge: E) = {
    addVertex(node1)
    addVertex(node2)
    graph.addEdge(node1, node2, Edge(edge))
  }

  def edgesOf(node : N) : Iterator[~>[~[N, E], N]]= {
    toEdgeIterator(graph.edgesOf(node))
  }

  private def toEdgeIterator(edges: Iterable[Edge[E]])  : Iterator[~>[~[N, E], N]] = {
    edges.map { edge =>
      val node1 = graph.getEdgeSource(edge)
      val node2 = graph.getEdgeTarget(edge)
      node1 ~ edge.edge ~> node2
    }.iterator
  }

  def edges : Iterator[~>[~[N, E], N]]= {
    toEdgeIterator(graph.edgeSet())
  }

  def + (other : Graph[N, E]) : Graph[N, E] = {
    var newGraph = graph.clone().asInstanceOf[DefaultDirectedGraph[N, Edge[E]]]
    Graphs.addGraph(newGraph, other.graph)
    new Graph(newGraph)
  }

  def topoIter = {
    new TopologicalOrderIterator[N, Edge[E]](graph)
  }

  override def toString = {
    graph.toString
  }
}

/**
 * Example:
 * Graph(1 ~ 2 ~> 4 ~ 5 ~> 7, 8~9~>55, 11)
 * Will create a graph with:
 * nodes:
 * 1, 4, 7, 8, 55, 11
 * edge:
 * 2: (1->4)
 * 5: (4->7)
 * 9: (8->55)
 *
 */
object Graph {

  import GraphHelper._

  def apply[N, E](elems: Array[GraphElement]*) = {
    val backStoreGraph = empty[N, E]
    val graph = elems.foldLeft(backStoreGraph) { (g, array) =>
      array.foreach { elem =>
        elem match {
          case Node(node) =>
            g.addVertex(node.asInstanceOf[N])
          case NodeConnection(node1, node2, edge) =>
            g.addEdge(node1.asInstanceOf[N], node2.asInstanceOf[N], edge.asInstanceOf[E])
        }
      }
      g
    }
    graph
  }

  def empty[N, E] = {
    new Graph(new DefaultDirectedGraph[N, Edge[E]](classOf[Edge[E]]))
  }

  def main (args: Array[String]) {
    val g = Graph(1~2~>4, 1~5~>4, 4~2~>8~2~>9)

    val out = new ObjectOutputStream(new ByteArrayOutputStream());
    out.writeObject(g);



    val bolt = TaskDescription(classOf[SOLBolt], 3)
    val bolt2 = TaskDescription(classOf[SOLBolt], 3)
    Console.println(bolt.equals(bolt2))
    Console.println(g.toString)
  }

  implicit def toGraphElements(element: Any): Array[GraphElement] = {

    def toArray(input: Any, output: Array[Any]): Array[Any] = {
      var result = output
      input match {
        case node1 ~> node2 => {
          result = result :+ node2
          node1 match {
            case n ~ e =>
              result = result :+ e
              toArray(n, result)
            case _ =>
              result = result :+ null
              toArray(node1, result)
          }
        }
        case node =>
          result = result :+ node
          result
      }
    }

    element match {
      case _ ~> _ => {
        var result = toArray(element, Array.empty[Any]).reverse

        0.until(result.length / 2).foldLeft(Array.empty[GraphElement]) { (output, index) =>
          val node1 = result(index * 2)
          val edge = result(index * 2 + 1)
          val node2 = result(index * 2 + 2)
          output :+ NodeConnection(node1, node2, edge)
        }
      }
      case _ => {
        Array(Node(element))
      }
    }
  }

  implicit def any2NodeWithEdge[A](x: A): NodeWithEdge[A] = new NodeWithEdge(x)

  implicit def any2NodeToNode[A](x: A): NodeToNode[A] = new NodeToNode(x)

  //we ensure two edge never equal
  case class Edge[E](edge: E) {
    private val id: Int = new Object().hashCode()
    override def equals(that: Any) = id.equals(that.asInstanceOf[Edge[Any]].id)
  }

  case class ~[N, E](node: N, edge: E)

  case class ~>[Node1, Node2](node1: Node1, node2: Node2)
}

object GraphHelper {
  sealed trait GraphElement

  case class Node[N](node: N) extends GraphElement

  case class NodeConnection[N, E](node1: N, node2: N, edge: E) extends GraphElement

  /**
   * Helper class to support operator ~ and ~>
   */
  final class NodeWithEdge[A](val __leftOfArrow: A) extends AnyVal {
    def x = __leftOfArrow

    def ~[B](y: B): Graph.~[A, B] = Graph.~(__leftOfArrow, y)
  }

  final class NodeToNode[A](val __leftOfArrow: A) extends AnyVal {
    def x = __leftOfArrow

    def ~>[B](y: B): Graph.~>[A, B] = Graph.~>(__leftOfArrow, y)
  }
}
