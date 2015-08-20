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

package io.gearpump.util

import scala.collection.mutable
import scala.language.implicitConversions

/**
 * Application DAG
 */
class Graph[N, E](vertexList: List[N], edgeList: List[(N, E, N)]) extends Serializable{

  private val _vertices = mutable.Set.empty[N] ++ vertexList
  private val _edges = mutable.Set.empty[(N, E, N)] ++ edgeList

  def addVertex(vertex : N): Unit = {
    _vertices.add(vertex)
  }

  def vertices: List[N] = {
    _vertices.toList
  }

  def outDegreeOf(node : N): Int = {
    _edges.count(_._1 == node)
  }

  def inDegreeOf(node: N): Int = {
    _edges.count(_._3 == node)
  }

  def outgoingEdgesOf(node : N): List[(N, E, N)]  = {
    _edges.toList.filter(_._1 == node)
  }

  def incomingEdgesOf(node: N): List[(N, E, N)] = {
    _edges.toList.filter(_._3 == node)
  }

  def removeVertex(node: N): Unit = {
    _vertices.remove(node)
    val toBeRemoved = incomingEdgesOf(node) ++ outgoingEdgesOf(node)
    toBeRemoved.foreach(_edges.remove(_))
  }

  def addEdge(node1 : N, edge: E, node2: N): Unit = {
    addVertex(node1)
    addVertex(node2)
    _edges.add((node1, edge, node2))
  }

  /**
   * Map a graph to a new graph, with vertex converted to a new type
   * @param fun
   * @tparam NewNode
   * @return
   */
  def mapVertex[NewNode](fun: N => NewNode): Graph[NewNode, E] = {
    val vertexMap: Map[N, NewNode] = vertices.map(node => (node, fun(node))).toMap
    val newNodes = vertexMap.values.toList

    val newEdges = edges.map { edge =>
      (vertexMap(edge._1), edge._2, vertexMap(edge._3))
    }
    new Graph(newNodes, newEdges)
  }

  /**
   * Map a graph to a new graph, with edge converted to new type
   * @param fun
   * @tparam NewEdge
   * @return
   */
  def mapEdge[NewEdge](fun: (N, E, N) => NewEdge): Graph[N, NewEdge] = {
    val newEdges =  edges.map {edge =>
      (edge._1, fun(edge._1, edge._2, edge._3), edge._3)
    }
    new Graph(vertices, newEdges)
  }


  def edgesOf(node : N): List[(N, E, N)] = {
    (incomingEdgesOf(node) ++ outgoingEdgesOf(node)).toSet.toList
  }

  def edges: List[(N, E, N)] = {
    _edges.toList
  }

  def addGraph(other : Graph[N, E]) : Graph[N, E] = {
    (vertices ++ other.vertices).foreach(addVertex(_))
    (edges ++ other.edges).foreach(edge =>addEdge(edge._1, edge._2, edge._3))
    this
  }

  def copy: Graph[N, E] = {
    new Graph(vertices, edges)
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

  def subGraph(node: N): Graph[N, E] = {
    val newGraph = Graph.empty[N, E]
    for (edge <- edgesOf(node)) {
      newGraph.addEdge(edge._1, edge._2, edge._3)
    }
    newGraph
  }

  def replaceVertex(node: N, newNode: N): Graph[N, E] = {
    for (edge <- incomingEdgesOf(node)) {
      addEdge(edge._1, edge._2, newNode)
    }

    for (edge <- outgoingEdgesOf(node)) {
      addEdge(newNode, edge._2, edge._3)
    }
    removeVertex(node)
    this
  }

  private def removeZeroInDegree: List[N] = {
    val toBeRemoved = vertices.filter(inDegreeOf(_) == 0)
    toBeRemoved.foreach(removeVertex(_))
    toBeRemoved
  }

  /**
   * Return an iterator of vertex in topological order
   */
  def topologicalOrderIterator: Iterator[N] = {
    val newGraph = copy
    var output = List.empty[N]

    while(!newGraph.isEmpty) {
      output ++= newGraph.removeZeroInDegree
    }
    output.iterator
  }


  /**
   * Generate a level map for each vertex
   * withholding: if vertex A -> B, then level(A) < level(B)
   */
  def vertexHierarchyLevelMap(): Map[N, Int] = {
    val newGraph = copy
    var output = Map.empty[N, Int]
    var level = 0
    while(!newGraph.isEmpty) {
      output ++= newGraph.removeZeroInDegree.map((_, level)).toMap
      level += 1
    }
    output
  }

  override def toString = {
    Map("vertices" -> vertices.mkString(","),
    "edges" -> edges.mkString(",")).toString()
  }
}

object Graph {

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
  def apply[N, E](elems: Path[_ <: N, _ <: E]*): Graph[N, E] = {
    val graph = empty[N, E]
    elems.foreach{ path =>
      path.updategraph(graph)
    }
    graph
  }

  def apply[N , E](vertices: List[N], edges: List[(N, E, N)]): Graph[N, E] = {
    new Graph(vertices, edges)
  }

  def unapply[N, E](graph: Graph[N, E]): Option[(List[N], List[(N, E, N)])] = {
    Some((graph.vertices, graph.edges))
  }

  def empty[N, E] = {
    new Graph(List.empty[N], List.empty[(N, E, N)])
  }

  class Path[N, +E](path: List[Either[N, E]]) {

    def ~[Edge >: E](edge: Edge): Path[N, Edge] = {
      new Path(path :+ Right(edge))
    }

    def ~>[Node >: N](node: Node): Path[Node, E] = {
      new Path(path :+ Left(node))
    }

    def to[Node >: N, Edge >: E](node: Node, edge: Edge): Path[Node, Edge] = {
      this ~ edge ~> node
    }

    private[Graph] def updategraph[Node >: N, Edge >: E](graph: Graph[Node, Edge]): Unit = {
      val nodeEdgePair: Tuple2[Option[N], Option[E]] = (None, None)
      path.foldLeft(nodeEdgePair) { (pair, either) =>
        val (lastNode, lastEdge) = pair
        either match {
          case Left(node) =>
            graph.addVertex(node)
            if (lastNode.isDefined) {
              graph.addEdge(lastNode.get, lastEdge.getOrElse(null.asInstanceOf[Edge]), node)
            }
            (Some(node), None)
          case Right(edge) =>
            (lastNode, Some(edge))
        }
      }
    }
  }

  object Path {
    implicit def anyToPath[N, E](any: N): Path[N, E] = Node(any)
  }

  implicit class Node[N, E](self: N) extends Path[N, E](List(Left(self))) {
    
    override def ~[Edge](edge: Edge): Path[N, Edge] = {
      new Path(List(Left(self), Right(edge)))
    }

    override def ~>[Node >: N](node: Node): Path[Node, E] = {
      new NodeList(List(self, node))
    }

    override def to[Node >: N, Edge >: E](node: Node, edge: Edge): Path[Node, Edge] = {
      this ~ edge ~> node
    }
  }

  class NodeList[N, E](nodes: List[N]) extends Path[N, E](nodes.map(Left(_))) {
    override def ~[Edge](edge: Edge): Path[N, Edge] = {
      new Path(nodes.map(Left(_)) :+ Right(edge))
    }

    override def ~>[Node >: N](node: Node): Path[Node, E] = {
      new NodeList(nodes :+ node)
    }

    override def to[Node >: N, Edge >: E](node: Node, edge: Edge): Path[Node, Edge] = {
      this ~ edge ~> node
    }
  }
}