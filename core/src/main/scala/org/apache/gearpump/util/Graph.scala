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

class Graph[N, E](private[Graph] val graph : DefaultDirectedGraph[N, Edge[E]]) extends Serializable{
  import org.apache.gearpump.util.Graph._

  def addVertex(vertex : N): Unit = {
    graph.addVertex(vertex)
  }

  def vertices: Iterable[N] = {
    graph.vertexSet()
  }

  def outDegreeOf(node : N): Int = {
    graph.outDegreeOf(node)
  }

  def inDegreeOf(node: N): Int = {
    graph.inDegreeOf(node)
  }

  def outgoingEdgesOf(node : N): Array[(N, E, N)]  = {
    toEdgeArray(graph.outgoingEdgesOf(node))
  }

  def incomingEdgesOf(node: N): Array[(N, E, N)] = {
    toEdgeArray(graph.incomingEdgesOf(node))
  }

  def removeVertex(node: N): Unit = {
    graph.removeVertex(node)
  }

  def addEdge(node1 : N, edge: E, node2: N): Unit = {
    addVertex(node1)
    addVertex(node2)
    graph.addEdge(node1, node2, Edge(edge))
  }

  /**
   * Map a graph to a new graph, with vertex converted to a new type
   * @param fun
   * @tparam NewNode
   * @return
   */
  def mapVertex[NewNode](fun: N => NewNode): Graph[NewNode, E] = {
    val vertexMap = vertices.foldLeft(Map.empty[N, NewNode]){(map, vertex) =>
      map + (vertex -> fun(vertex))
    }
    val newGraph = Graph.empty[NewNode, E]
    vertexMap.values.foreach(newGraph.addVertex(_))
    edges.foreach {edgeWithEnds =>
      val (node1, edge, node2) = edgeWithEnds
      newGraph.addEdge(vertexMap(node1), edge, vertexMap(node2))
    }
    newGraph
  }

  /**
   * Map a graph to a new graph, with edge converted to new type
   * @param fun
   * @tparam NewEdge
   * @return
   */
  def mapEdge[NewEdge](fun: E => NewEdge): Graph[N, NewEdge] = {
    val newGraph = Graph.empty[N, NewEdge]
    vertices.foreach(newGraph.addVertex(_))
    edges.foreach {edgeWithEnds =>
      val (node1, edge, node2) = edgeWithEnds
      newGraph.addEdge(node1, fun(edge), node2)
    }
    newGraph
  }

  def edgesOf(node : N): Array[(N, E, N)] = {
    toEdgeArray(graph.edgesOf(node))
  }

  private def toEdgeArray(edges: Iterable[Edge[E]]): Array[(N, E, N)] = {
    edges.map { edge =>
      val node1 = graph.getEdgeSource(edge)
      val node2 = graph.getEdgeTarget(edge)
      (node1, edge.edge, node2)
    }.toArray
  }

  def edges: Array[(N, E, N)] = {
    toEdgeArray(graph.edgeSet())
  }

  def addGraph(other : Graph[N, E]) : Graph[N, E] = {
    val newGraph = graph.clone().asInstanceOf[DefaultDirectedGraph[N, Edge[E]]]
    Graphs.addGraph(newGraph, other.graph)
    new Graph(newGraph)
  }

  def isEmpty: Boolean = {
    val vertexCount = vertices.size
    val edgeCount = edges.length
    if (vertexCount + edgeCount == 0) {
      true
    } else {
      false
    }
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
      array.foreach {
        case Node(node) =>
          g.addVertex(node.asInstanceOf[N])
        case NodeConnection(node1, edge, node2) =>
          g.addEdge(node1.asInstanceOf[N], edge.asInstanceOf[E], node2.asInstanceOf[N])
      }
      g
    }
    graph
  }

  def apply[N, E](backStoreGraph: Graph[N, E], edges : (N, E, N)*) = {
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
        case node1 ~> node2 =>
          result = result :+ node2
          node1 match {
            case n ~ e =>
              result = result :+ e
              toArray(n, result)
            case _ =>
              result = result :+ null
              toArray(node1, result)
          }
        case node =>
          result = result :+ node
          result
      }
    }

    element match {
      case _ ~> _ =>
        val result = toArray(element, Array.empty[Any]).reverse

        0.until(result.length - 1, 2).foldLeft(Array.empty[GraphElement]) { (output, index) =>
          val node1 = result(index)
          val edge = result(index + 1)
          val node2 = result(index + 2)
          output :+ NodeConnection(node1, edge, node2)
        }
      case _ =>
        Array(Node(element))
    }
  }

  implicit def any2NodeWithEdge[A](x: A): NodeWithEdge[A] = new NodeWithEdge(x)

  implicit def any2NodeToNode[A](x: A): NodeToNode[A] = new NodeToNode(x)

  //we ensure two edge instance never equal
  case class Edge[E](edge: E)  extends ReferenceEqual

  case class ~[N, E](node: N, edge: E)

  case class ~>[Node1, Node2](node1: Node1, node2: Node2)


  /**
   * Generate a level map for each vertex
   * withholding: if vertex A -> B, then level(A) < level(B)
   *
   * @param graph
   * @tparam N
   * @tparam E
   * @return
   */
  def vertexHierarchyLevelMap[N, E](graph: Graph[N, E]): Map[N, Int] = {
    val levelGraph = graph.graph.clone().asInstanceOf[DefaultDirectedGraph[N, Edge[E]]]

    var levelMap = Map.empty[N, Int]
    var currentLevel = 0
    while (levelGraph.vertexSet().size() > 0) {
      val nodes = vertexsZeroInDegree(levelGraph)
      levelMap = nodes.foldLeft(levelMap)((map, node) => map + (node -> currentLevel))
      levelGraph.removeAllVertices(nodes)
      currentLevel += 1
    }
    levelMap
  }

  import scala.collection.JavaConverters._
  import scala.collection.JavaConversions
  private def vertexsZeroInDegree[N, E](graph: DefaultDirectedGraph[N, Edge[E]]) = {
    graph.vertexSet().asScala.filter(node => graph.inDegreeOf(node) == 0)
  }
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
