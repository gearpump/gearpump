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

import org.apache.gearpump.util.GraphHelper.{GraphElement, Node}
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{PropSpec, Matchers}

import org.apache.gearpump.util.Graph._

class GraphSpec extends PropSpec with PropertyChecks with Matchers {

  case class Vertex(id: Int)
  case class Edge(from: Int, to: Int)

  val verticesNumGen = Gen.chooseNum[Int](1, 100)
  property("Graph with no edges should be built correctly") {
    forAll(verticesNumGen) {
      (num: Int) =>
        val vertices = 0.until(num).map(Vertex).toArray
        val graphElements = vertices.map(Graph.toGraphElements)
        val graph: Graph[Vertex, Edge] = Graph(graphElements: _*)
        graph.vertices.toArray shouldBe vertices
        vertices.foreach { vertex =>
          graph.outDegreeOf(vertex) shouldBe 0
          graph.edgesOf(vertex) shouldBe empty
          graph.outgoingEdgesOf(vertex) shouldBe empty
        }
        graph.edges shouldBe empty
    }
  }

  property("Graph with vertices and edges should be built correctly") {
    forAll(verticesNumGen) {
      (num: Int) =>
        val vertices: Array[Vertex] = 0.until(num).map(Vertex).toArray
        val genEdge = for {
          from <- Gen.chooseNum[Int](0, num - 1)
          to   <- Gen.chooseNum[Int](0, num - 1)
        } yield Edge(from, to)
        val genEdges: Gen[Array[Edge]] = Gen.containerOf[Array, Edge](genEdge).map(edges => edges.toSet.toArray)

        forAll(genEdges) {
          (edges: Array[Edge]) =>
            var graphElements = Array.empty[Array[GraphElement]]
            val outDegrees = new Array[Int](vertices.length)
            val outGoingEdges = vertices.map(_ => Array.empty[(Vertex, Edge, Vertex)])
            val edgesOf = vertices.map(_ => Array.empty[(Vertex, Edge, Vertex)])
            vertices.foreach { v =>
              graphElements :+= Graph.toGraphElements(v)
            }
            edges.foreach { e =>
              val from = vertices(e.from)
              val to = vertices(e.to)
              graphElements :+= Graph.toGraphElements(from ~ e ~> to)
              outDegrees(e.from) += 1
              outGoingEdges(e.from) :+= (from, e, to)
              edgesOf(e.from) :+= (from, e, to)
              if (e.from != e.to) {
                edgesOf(e.to) :+=(from, e, to)
              }
            }

            val graph: Graph[Vertex, Edge] = Graph(graphElements: _*)
            graph.vertices should contain theSameElementsAs vertices
            graph.edges.toArray should contain theSameElementsAs edges.map(e => (vertices(e.from), e, vertices(e.to)))

            0.until(vertices.size).foreach { i =>
              val v = vertices(i)
              graph.outDegreeOf(v) shouldBe outDegrees(i)
              graph.outgoingEdgesOf(v) should contain theSameElementsAs outGoingEdges(i)
              graph.edgesOf(v) should contain theSameElementsAs edgesOf(i)
            }
        }
    }
  }

  property("Check empty graph") {
    val graph = Graph.empty[String, String]
    assert(graph.isEmpty)
  }

}
