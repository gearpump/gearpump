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

import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.util.Graph.Edge
import org.jgrapht.Graphs
import org.jgrapht.graph.DefaultDirectedGraph
import org.jgrapht.traverse.TopologicalOrderIterator
import upickle.{Reader, Js, Writer}
import org.slf4j.Logger

import scala.collection.JavaConversions._
import scala.language.implicitConversions

/**
 * Application DAG
 */

class Graph[N , E](private[Graph] val graph : DefaultDirectedGraph[N, Edge[E]]) extends Serializable{
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
  def mapEdge[NewEdge](fun: (N, E, N) => NewEdge): Graph[N, NewEdge] = {
    val newGraph = Graph.empty[N, NewEdge]
    vertices.foreach(newGraph.addVertex(_))
    edges.foreach {edgeWithEnds =>
      val (node1, edge, node2) = edgeWithEnds
      val newEdge = fun(node1, edge, node2)
      newGraph.addEdge(node1, newEdge, node2)
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

  /**
   * Reserved for java usage
   * @param elems
   * @tparam N
   * @tparam E
   * @return
   */
  def apply[N, E](elems: java.util.List[Path[_ <: N, _ <: E]]): Graph[N, E] = {
    val graph = empty[N, E]
    elems.foreach{ path =>
      path.updategraph(graph)
    }
    graph
  }

  def empty[N, E] = {
    new Graph(new DefaultDirectedGraph[N, Edge[E]](classOf[Edge[E]]))
  }

  //we ensure two edge instance never equal
  case class Edge[E](edge: E)  extends ReferenceEqual

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

  implicit val writer: Writer[Graph[Int, String]] = upickle.Writer[Graph[Int, String]] {

    case dag =>
      val vertices = dag.vertices.map(processorId => {
        Js.Num(processorId)
      })

      val edges = dag.edges.map(f => {
        var (node1, edge, node2) = f
        Js.Arr(Js.Num(node1), Js.Str(edge), Js.Num(node2))
      })

      Js.Obj(
        ("vertices", Js.Arr(vertices.toSeq: _*)),
        ("edges", Js.Arr(edges.toSeq: _*)))

  }

  implicit val reader: Reader[Graph[Int, String]] = upickle.Reader[Graph[Int, String]] {

    case r: Js.Obj =>
      val graph = Graph.empty[Int, String]
      r.value.foreach(pair => {
        val (member, value) = pair
        member match {
          case "vertices" =>
            val vertices = upickle.readJs[Array[Int]](value)
            vertices.foreach(graph.addVertex(_))
          case "edges" =>
            val paths = upickle.readJs[Array[(Int,String,Int)]](value)
            paths.foreach(tuple => {
              val (node1, edge, node2) = tuple
              graph.addEdge(node1, edge, node2)
            })
        }
      })
      graph
  }

}
