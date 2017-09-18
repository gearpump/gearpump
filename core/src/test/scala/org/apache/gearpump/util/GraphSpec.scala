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

import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

import org.apache.gearpump.util.Graph.{Node, Path}

class GraphSpec extends PropSpec with PropertyChecks with Matchers {

  case class Vertex(id: Int)
  case class Edge(from: Int, to: Int)

  val vertexCount = 100

  property("Graph with no edges should be built correctly") {
    val vertexSet = Set("A", "B", "C")
    val graph = Graph(vertexSet.toSeq.map(Node): _*)
    graph.getVertices.toSet shouldBe vertexSet
  }

  property("Graph with vertices and edges should be built correctly") {
    val vertices: Array[Vertex] = 0.until(vertexCount).map(Vertex).toArray
    val genEdge = for {
      from <- Gen.chooseNum[Int](0, vertexCount - 1)
      to <- Gen.chooseNum[Int](0, vertexCount - 1)
    } yield Edge(from, to)

    var graphElements = Array.empty[Path[Vertex, _ <: Edge]]
    val outDegrees = new Array[Int](vertices.length)
    val outGoingEdges = vertices.map(_ => Set.empty[(Vertex, Edge, Vertex)])
    val edgesOf = vertices.map(_ => Set.empty[(Vertex, Edge, Vertex)])
    vertices.foreach { v =>
      graphElements :+= Node(v)
    }

    forAll(genEdge) {
      e: Edge =>
        val from = vertices(e.from)
        val to = vertices(e.to)
        graphElements :+= from ~ e ~> to
        outDegrees(e.from) += 1

        val nodeEdgeNode = (from, e, to)
        outGoingEdges(e.from) += nodeEdgeNode

        edgesOf(e.from) += nodeEdgeNode
        edgesOf(e.to) += nodeEdgeNode
    }

    val graph: Graph[Vertex, Edge] = Graph(graphElements: _*)
    graph.getVertices should contain theSameElementsAs vertices

    0.until(vertices.size).foreach { i =>
      val v = vertices(i)
      graph.outgoingEdgesOf(v) should contain theSameElementsAs outGoingEdges(i)
      graph.edgesOf(v).sortBy(_._1.id)
      graph.edgesOf(v) should contain theSameElementsAs edgesOf(i)
    }
  }

  property("Check empty graph") {
    val graph = Graph.empty[String, String]
    assert(graph.isEmpty)
  }

  property("check level map for a graph") {
    val graph = Graph.empty[String, String]

    val defaultEdge = "edge"

    graph.addVertex("A")
    graph.addVertex("B")
    graph.addVertex("C")

    graph.addEdge("A", defaultEdge, "B")
    graph.addEdge("B", defaultEdge, "C")
    graph.addEdge("A", defaultEdge, "C")

    graph.addVertex("D")
    graph.addVertex("E")
    graph.addVertex("F")

    graph.addEdge("D", defaultEdge, "E")
    graph.addEdge("E", defaultEdge, "F")
    graph.addEdge("D", defaultEdge, "F")

    graph.addEdge("C", defaultEdge, "E")

    val levelMap = graph.vertexHierarchyLevelMap()

    // Check whether the rule holds: : if vertex A -> B, then level(A) < level(B)
    assert(levelMap("A") < levelMap("B"))
    assert(levelMap("A") < levelMap("C"))
    assert(levelMap("B") < levelMap("C"))

    assert(levelMap("D") < levelMap("E"))
    assert(levelMap("D") < levelMap("F"))
    assert(levelMap("E") < levelMap("F"))

    assert(levelMap("C") < levelMap("F"))
  }

  property("copy should return a immutalbe new Graph") {
    val graph = Graph.empty[String, String]
    val defaultEdge = "edge"
    graph.addVertex("A")
    graph.addVertex("B")
    graph.addEdge("A", defaultEdge, "B")

    val newGraph = graph.copy
    newGraph.addVertex("C")

    assert(!graph.getVertices.toSet.contains("C"), "Graph should be immutable")
  }

  property("subGraph should return a sub-graph for certain vertex") {
    val graph = Graph.empty[String, String]
    val defaultEdge = "edge"
    graph.addVertex("A")
    graph.addVertex("B")
    graph.addVertex("C")
    graph.addEdge("A", defaultEdge, "B")
    graph.addEdge("B", defaultEdge, "C")
    graph.addEdge("A", defaultEdge, "C")

    val subGraph = graph.subGraph("C")
    assert(subGraph.outDegreeOf("A") != graph.outDegreeOf("A"))
  }

  property("replaceVertex should hold all upstream downstream relation for a vertex") {
    val graph = Graph.empty[String, String]
    val defaultEdge = "edge"
    graph.addVertex("A")
    graph.addVertex("B")
    graph.addVertex("C")
    graph.addEdge("A", defaultEdge, "B")
    graph.addEdge("B", defaultEdge, "C")

    val newGraph = graph.copy.replaceVertex("B", "D")
    assert(newGraph.inDegreeOf("D") == graph.inDegreeOf("B"))
    assert(newGraph.outDegreeOf("D") == graph.outDegreeOf("B"))
  }

  property("Cycle detecting should work properly") {
    val graph = Graph.empty[String, String]
    val defaultEdge = "edge"
    graph.addVertex("A")
    graph.addVertex("B")
    graph.addVertex("C")
    graph.addEdge("A", defaultEdge, "B")
    graph.addEdge("B", defaultEdge, "C")

    assert(!graph.hasCycle())

    graph.addEdge("C", defaultEdge, "B")
    assert(graph.hasCycle())

    graph.addEdge("C", defaultEdge, "A")
    assert(graph.hasCycle())
  }

  property("topologicalOrderIterator and topologicalOrderWithCirclesIterator method should " +
    "return equal order of graph with no circle") {
    val graph = Graph(1 ~> 2 ~> 3, 4 ~> 2, 2 ~> 5)
    val topoNoCircles = graph.topologicalOrderIterator
    val topoWithCircles = graph.topologicalOrderWithCirclesIterator

    assert(topoNoCircles.zip(topoWithCircles).forall(x => x._1 == x._2))
  }

  property("Topological sort of graph with circles should work properly") {
    val graph = Graph(0 ~> 1 ~> 3 ~> 4 ~> 6 ~> 5 ~> 7,
      4 ~> 1, 1 ~> 2 ~> 4, 7 ~> 6, 8 ~> 2, 6 ~> 9, 4 ~> 10)
    val topoWithCircles = graph.topologicalOrderWithCirclesIterator
    val trueTopoWithCircles = Iterator[Int](0, 8, 1, 3, 4, 2, 6, 5, 7, 10, 9)

    assert(trueTopoWithCircles.zip(topoWithCircles).forall(x => x._1 == x._2))
  }

  property("Hierarchy level map should handle graph with cycles") {
    val graph = Graph(0 ~> 1 ~> 2 ~> 3 ~> 4, 3 ~>1)
    val map = graph.vertexHierarchyLevelMap()
    assert(map(0) < map(1))
    assert(map(1) < map(2))
    assert(map(2) < map(3))
    assert(map(3) < map(4))
  }

  property("Duplicated edges detecting should work properly") {
    val graph = Graph.empty[String, String]
    val defaultEdge = "edge"
    val anotherEdge = "edge2"
    graph.addVertex("A")
    graph.addVertex("B")
    graph.addEdge("A", defaultEdge, "B")

    assert(!graph.hasDuplicatedEdge())

    graph.addEdge("A", anotherEdge, "B")

    assert(graph.hasDuplicatedEdge())
  }
}
