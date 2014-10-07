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

package org.apache.gearpump.util

import org.apache.gearpump.util.Graph.Edge
import org.jgrapht.Graphs
import org.jgrapht.graph.DefaultDirectedGraph
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.collection.JavaConversions._
import scala.language.implicitConversions


/**
 * Application DAG
 */

import java.io.ByteArrayOutputStream

class Graph[N, E](private[Graph] val graph : DefaultDirectedGraph[N, Edge[E]]) extends Serializable{
  import org.apache.gearpump.util.Graph._

  def addVertex(vertex : N) : Unit = {
    graph.addVertex(vertex)
  }

  def vertex : Iterable[N] = {
    graph.vertexSet()
  }

  def outDegreeOf(node : N) = {
    graph.outDegreeOf(node)
  }

  def outgoingEdgesOf(node : N)  = {
    toEdgeArray(graph.outgoingEdgesOf(node))
  }

  def addEdge(node1 : N, edge: E, node2: N) = {
    addVertex(node1)
    addVertex(node2)
    graph.addEdge(node1, node2, Edge(edge))
  }

  def edgesOf(node : N) = {
    toEdgeArray(graph.edgesOf(node))
  }

  private def toEdgeArray(edges: Iterable[Edge[E]])  : Array[(N, E,  N)] = {
    edges.map { edge =>
      val node1 = graph.getEdgeSource(edge)
      val node2 = graph.getEdgeTarget(edge)
      (node1, edge.edge, node2)
    }.toArray
  }

  def edges = {
    toEdgeArray(graph.edgeSet())
  }

  def addGraph(other : Graph[N, E]) : Graph[N, E] = {
    var newGraph = graph.clone().asInstanceOf[DefaultDirectedGraph[N, Edge[E]]]
    Graphs.addGraph(newGraph, other.graph)
    new Graph(newGraph)
  }

  /**
   * Return an iterator of vertex in topological order
   */
  def topologicalOrderIterator = {
    new TopologicalOrderIterator[N, Edge[E]](graph)
  }

  override def toString = {
    graph.toString
  }
}

object Graph {

  import org.apache.gearpump.util.GraphHelper._

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
  def apply[N, E](elems: Array[GraphElement]*) = {
    val backStoreGraph = empty[N, E]
    val graph = elems.foldLeft(backStoreGraph) { (g, array) =>
      array.foreach { elem =>
        elem match {
          case Node(node) =>
            g.addVertex(node.asInstanceOf[N])
          case NodeConnection(node1, edge, node2) =>
            g.addEdge(node1.asInstanceOf[N], edge.asInstanceOf[E], node2.asInstanceOf[N])
        }
      }
      g
    }
    graph
  }

  def apply[N, E](edges : Tuple3[N, E, N]*) = {
    val backStoreGraph = empty[N, E]
    edges.foreach {nodeEdgeNode =>
      val (node1, edge, node2) = nodeEdgeNode
      backStoreGraph.addEdge(node1, edge, node2)
    }
  }


  def empty[N, E] = {
    new Graph(new DefaultDirectedGraph[N, Edge[E]](classOf[Edge[E]]))
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

        0.until(result.length - 1, 2).foldLeft(Array.empty[GraphElement]) { (output, index) =>
          val node1 = result(index)
          val edge = result(index + 1)
          val node2 = result(index + 2)
          output :+ NodeConnection(node1, edge, node2)
        }
      }
      case _ => {
        Array(Node(element))
      }
    }
  }

  implicit def any2NodeWithEdge[A](x: A): NodeWithEdge[A] = new NodeWithEdge(x)

  implicit def any2NodeToNode[A](x: A): NodeToNode[A] = new NodeToNode(x)

  //we ensure two edge instance never equal
  case class Edge[E](edge: E)  extends ReferenceEqual

  case class ~[N, E](node: N, edge: E)

  case class ~>[Node1, Node2](node1: Node1, node2: Node2)
}

object GraphHelper {
  sealed trait GraphElement

  case class Node[N](node: N) extends GraphElement

  case class NodeConnection[N, E](node1: N, edge: E, node2: N) extends GraphElement

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
