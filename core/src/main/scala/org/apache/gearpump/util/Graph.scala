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

package org.apache.gearpump.util
import scala.annotation.tailrec
import scala.collection.mutable
import scala.language.implicitConversions

/**
 * Generic mutable Graph libraries.
 */
class Graph[N, E](vertexList: List[N], edgeList: List[(N, E, N)]) extends Serializable {

  private val _vertices = mutable.Set.empty[N]
  private val _edges = mutable.Set.empty[(N, E, N)]

  // This is used to ensure the output of this Graph is always stable
  // Like method vertices(), or edges()
  private var _indexs = Map.empty[Any, Int]
  private var _nextIndex = 0
  private def nextId: Int = {
    val result = _nextIndex
    _nextIndex += 1
    result
  }

  private def init(): Unit = {
    Option(vertexList).getOrElse(List.empty[N]).foreach(addVertex(_))
    Option(edgeList).getOrElse(List.empty[(N, E, N)]).foreach(addEdge(_))
  }

  init()

  /**
   * Add a vertex
   * Current Graph is changed.
   */
  def addVertex(vertex: N): Unit = {
    val result = _vertices.add(vertex)
    if (result) {
      _indexs += vertex -> nextId
    }
  }

  /**
   * Add a edge
   * Current Graph is changed.
   */
  def addEdge(edge: (N, E, N)): Unit = {
    val result = _edges.add(edge)
    if (result) {
      _indexs += edge -> nextId
    }
  }

  /**
   * return all vertices.
   * The result is stable
   */
  def vertices: List[N] = {
    // Sorts the vertex so that we can keep the order for mapVertex
    _vertices.toList.sortBy(_indexs(_))
  }

  /**
   * out degree
   */
  def outDegreeOf(node: N): Int = {
    edges.count(_._1 == node)
  }

  /**
   * in degree
   */
  def inDegreeOf(node: N): Int = {
    edges.count(_._3 == node)
  }

  /**
   * out going edges.
   */
  def outgoingEdgesOf(node: N): List[(N, E, N)] = {
    edges.filter(_._1 == node)
  }

  /**
   * incoming edges.
   */
  def incomingEdgesOf(node: N): List[(N, E, N)] = {
    edges.filter(_._3 == node)
  }

  /**
   * Remove vertex
   * Current Graph is changed.
   */
  def removeVertex(node: N): Unit = {
    _vertices.remove(node)
    _indexs -= node
    val toBeRemoved = incomingEdgesOf(node) ++ outgoingEdgesOf(node)
    toBeRemoved.foreach(removeEdge(_))
  }

  /**
   * Remove edge
   * Current Graph is changed.
   */
  private def removeEdge(edge: (N, E, N)): Unit = {
    _indexs -= edge
    _edges.remove(edge)
  }

  /**
   * add edge
   * Current Graph is changed.
   */
  def addEdge(node1: N, edge: E, node2: N): Unit = {
    addVertex(node1)
    addVertex(node2)
    addEdge((node1, edge, node2))
  }

  /**
   * Map a graph to a new graph, with vertex converted to a new type
   * Current Graph is not changed.
   */
  def mapVertex[NewNode](fun: N => NewNode): Graph[NewNode, E] = {
    val vertexes = vertices.map(node => (node, fun(node)))

    val vertexMap: Map[N, NewNode] = vertexes.toMap

    val newEdges = edges.map { edge =>
      (vertexMap(edge._1), edge._2, vertexMap(edge._3))
    }
    new Graph(vertexes.map(_._2), newEdges)
  }

  /**
   * Map a graph to a new graph, with edge converted to new type
   * Current graph is not changed.
   */
  def mapEdge[NewEdge](fun: (N, E, N) => NewEdge): Graph[N, NewEdge] = {
    val newEdges = edges.map { edge =>
      (edge._1, fun(edge._1, edge._2, edge._3), edge._3)
    }
    new Graph(vertices, newEdges)
  }

  /**
   * edges connected to node
   */
  def edgesOf(node: N): List[(N, E, N)] = {
    (incomingEdgesOf(node) ++ outgoingEdgesOf(node)).toSet[(N, E, N)].toList.sortBy(_indexs(_))
  }

  /**
   * all edges
   */
  def edges: List[(N, E, N)] = {
    _edges.toList.sortBy(_indexs(_))
  }

  /**
   * Add another graph
   * Current graph is changed.
   */
  def addGraph(other: Graph[N, E]): Graph[N, E] = {
    (vertices ++ other.vertices).foreach(addVertex(_))
    (edges ++ other.edges).foreach(edge => addEdge(edge._1, edge._2, edge._3))
    this
  }

  /**
   * clone the graph
   */
  def copy: Graph[N, E] = {
    new Graph(vertices, edges)
  }

  /**
   * check empty
   */
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
   * sub-graph which contains current node and all neighbour
   * nodes and edges.
   */
  def subGraph(node: N): Graph[N, E] = {
    val newGraph = Graph.empty[N, E]
    for (edge <- edgesOf(node)) {
      newGraph.addEdge(edge._1, edge._2, edge._3)
    }
    newGraph
  }

  /**
   * replace vertex, the current Graph is mutated.
   */
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
    val toBeRemoved = vertices.filter(inDegreeOf(_) == 0).sortBy(_indexs(_))
    toBeRemoved.foreach(removeVertex(_))
    toBeRemoved
  }

  /**
   * Return an iterator of vertex in topological order
   * The node returned by Iterator is stable sorted.
   */
  def topologicalOrderIterator: Iterator[N] = {
    val newGraph = copy
    var output = List.empty[N]

    while (!newGraph.isEmpty) {
      output ++= newGraph.removeZeroInDegree
    }
    output.iterator
  }

  /**
   * Return all circles in graph.
   *
   * The reference of this algorithm is:
   * https://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
   */
  private def findCircles: mutable.MutableList[mutable.MutableList[N]] = {
    val inStack = mutable.Map.empty[N, Boolean]
    val stack = mutable.Stack[N]()
    val indexMap = mutable.Map.empty[N, Int]
    val lowLink = mutable.Map.empty[N, Int]
    var index = 0

    val circles = mutable.MutableList.empty[mutable.MutableList[N]]

    def tarjan(node: N): Unit = {
      indexMap(node) = index
      lowLink(node) = index
      index += 1
      inStack(node) = true
      stack.push(node)

      outgoingEdgesOf(node).foreach {
        edge => {
          if (!indexMap.contains(edge._3)) {
            tarjan(edge._3)
            if (lowLink.get(edge._3).get < lowLink.get(node).get) {
              lowLink(node) = lowLink(edge._3)
            }
          } else {
            if (inStack.get(edge._3).get && (indexMap.get(edge._3).get < lowLink.get(node).get)) {
              lowLink(node) = indexMap(edge._3)
            }
          }
        }
      }

      if (indexMap.get(node).get == lowLink.get(node).get) {
        val circle = mutable.MutableList.empty[N]
        var n = node
        do {
          n = stack.pop()
          inStack(n) = false
          circle += n
        } while (n != node)
        circles += circle
      }
    }

    vertices.foreach {
      node => {
        if (!indexMap.contains(node)) tarjan(node)
      }
    }

    circles
  }

  /**
   * Return an iterator of vertex in topological order of graph with circles
   * The node returned by Iterator is stable sorted.
   *
   * The reference of this algorithm is:
   * http://www.drdobbs.com/database/topological-sorting/184410262
   */
  def topologicalOrderWithCirclesIterator: Iterator[N] = {
    val circles = findCircles
    val newGraph = Graph.empty[mutable.MutableList[N], E]
    circles.foreach {
      circle => {
        newGraph.addVertex(circle)
      }
    }

    for (circle1 <- circles; circle2 <- circles; if circle1 != circle2) yield {
      for (node1 <- circle1; node2 <- circle2) yield {
        var edges = outgoingEdgesOf(node1)
        for (edge <- edges; if edge._3 == node2) yield {
          newGraph.addEdge(circle1, edge._2, circle2)
        }

        edges = outgoingEdgesOf(node2)
        for (edge <- edges; if edge._3 == node1) yield {
          newGraph.addEdge(circle2, edge._2, circle1)
        }
      }
    }

    val topo = newGraph.topologicalOrderIterator
    topo.flatMap(_.sortBy(_indexs(_)).iterator)
  }

  /**
   * check whether there is a loop
   */
  def hasCycle(): Boolean = {
    @tailrec
    def detectCycle(graph: Graph[N, E]): Boolean = {
      if (graph.edges.isEmpty) {
        false
      } else if (graph.vertices.nonEmpty && !graph.vertices.exists(graph.inDegreeOf(_) == 0)) {
        true
      } else {
        graph.removeZeroInDegree
        detectCycle(graph)
      }
    }

    detectCycle(copy)
  }

  /**
   * Check whether there are two edges connecting two nodes.
   */
  def hasDuplicatedEdge(): Boolean = {
    edges.groupBy(edge => (edge._1, edge._3)).values.exists(_.size > 1)
  }

  /**
   * Generate a level map for each vertex withholding:
   * {{{
   * if vertex A -> B, then level(A) -> level(B)
   * }}}
   */
  def vertexHierarchyLevelMap(): Map[N, Int] = {
    val newGraph = copy
    var output = Map.empty[N, Int]
    var level = 0
    while (!newGraph.isEmpty) {
      output ++= newGraph.removeZeroInDegree.map((_, level)).toMap
      level += 1
    }
    output
  }

  override def toString: String = {
    Map("vertices" -> vertices.mkString(","),
      "edges" -> edges.mkString(",")).toString()
  }
}

object Graph {

  /**
   * Example:
   *
   * {{{
   * Graph(1 ~ 2 ~> 4 ~ 5 ~> 7, 8~9~>55, 11)
   * Will create a graph with:
   * nodes:
   * 1, 4, 7, 8, 55, 11
   * edge:
   * 2: (1->4)
   * 5: (4->7)
   * 9: (8->55)
   * }}}
   */
  def apply[N, E](elems: Path[_ <: N, _ <: E]*): Graph[N, E] = {
    val graph = empty[N, E]
    elems.foreach { path =>
      path.updategraph(graph)
    }
    graph
  }

  def apply[N, E](vertices: List[N], edges: List[(N, E, N)]): Graph[N, E] = {
    new Graph(vertices, edges)
  }

  def unapply[N, E](graph: Graph[N, E]): Option[(List[N], List[(N, E, N)])] = {
    Some((graph.vertices, graph.edges))
  }

  def empty[N, E]: Graph[N, E] = {
    new Graph(List.empty[N], List.empty[(N, E, N)])
  }

  class Path[N, + E](path: List[Either[N, E]]) {

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