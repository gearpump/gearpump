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

package org.apache.gearpump.akkastream.graph

import akka.stream.{Shape, SinkShape, SourceShape}
import org.apache.gearpump.akkastream.GearAttributes
import org.apache.gearpump.akkastream.GearAttributes.{Local, Location, Remote}
import org.apache.gearpump.akkastream.GearpumpMaterializer.Edge
import org.apache.gearpump.akkastream.graph.GraphPartitioner.Strategy
import org.apache.gearpump.akkastream.module._
import akka.stream.impl.StreamLayout.Module
import akka.stream.impl.fusing.GraphStageModule
import akka.stream.impl.fusing.GraphStages.{MaterializedValueSource, SingleSource}
import akka.stream.impl.{SinkModule, SourceModule}
import org.apache.gearpump.util.Graph

/**
 *
 * GraphPartitioner is used to decide which part will be rendered locally
 * and which part should be rendered remotely.
 *
 * We will cut the graph based on the [[Strategy]] provided.
 *
 * For example, for the following graph, we can cut the graph to
 * two parts, each part will be a Sub Graph. The top SubGraph
 * can be materialized remotely. The bottom part can be materialized
 * locally.
 *
 *        AtomicModule2 -> AtomicModule4
 *           /|                 \
 *          /                    \
 * -----------cut line -------------cut line ----------
 *       /                        \
 *     /                           \|
 * AtomicModule1           AtomicModule5
 *     \                   /|
 *      \                 /
 *       \|              /
 *         AtomicModule3
 *
 * @see [[akka.stream.impl.MaterializerSession]] for more information of how Graph is organized.
 *
 */
class GraphPartitioner(strategy: Strategy) {
  def partition(moduleGraph: Graph[Module, Edge]): List[SubGraph] = {
    val graph = removeDummyModule(moduleGraph)
    val tags = tag(graph, strategy)
    doPartition(graph, tags)
  }

  private def doPartition(graph: Graph[Module, Edge], tags: Map[Module, Location]):
  List[SubGraph] = {
    val local = Graph.empty[Module, Edge]
    val remote = Graph.empty[Module, Edge]

    graph.getVertices.foreach{ module =>
      if (tags(module) == Local) {
        local.addVertex(module)
      } else {
        remote.addVertex(module)
      }
    }

    graph.getEdges.foreach{ nodeEdgeNode =>
      val (node1, edge, node2) = nodeEdgeNode
      (tags(node1), tags(node2)) match {
        case (Local, Local) =>
          local.addEdge(nodeEdgeNode)
        case (Remote, Remote) =>
          remote.addEdge(nodeEdgeNode)
        case (Local, Remote) =>
          node2 match {
            case bridge: BridgeModule[_, _, _] =>
              local.addEdge(node1, edge, node2)
            case _ =>
              // create a bridge module in between
              val bridge = new SourceBridgeModule[AnyRef, AnyRef]()
              val remoteEdge = Edge(bridge.outPort, edge.to)
              remote.addEdge(bridge, remoteEdge, node2)
              val localEdge = Edge(edge.from, bridge.inPort)
              local.addEdge(node1, localEdge, bridge)
          }
        case (Remote, Local) =>
          node1 match {
            case bridge: BridgeModule[_, _, _] =>
              local.addEdge(node1, edge, node2)
            case _ =>
              // create a bridge module in between
              val bridge = new SinkBridgeModule[AnyRef, AnyRef]()
              val remoteEdge = Edge(edge.from, bridge.inPort)
              remote.addEdge(node1, remoteEdge, bridge)
              val localEdge = Edge(bridge.outPort, edge.to)
              local.addEdge(bridge, localEdge, node2)
          }
      }
    }

    List(new RemoteGraph(remote), new LocalGraph(local))
  }

  private def tag(graph: Graph[Module, Edge], strategy: Strategy): Map[Module, Location] = {
    graph.getVertices.map{ vertex =>
      vertex -> strategy.apply(vertex)
    }.toMap
  }

  private def removeDummyModule(inputGraph: Graph[Module, Edge]): Graph[Module, Edge] = {
    val graph = inputGraph.copy
    val dummies = graph.getVertices.filter { module =>
      module match {
        case dummy: DummyModule =>
          true
        case _ =>
          false
      }
    }
    dummies.foreach(module => graph.removeVertex(module))
    graph
  }
}

object GraphPartitioner {

  type Strategy = PartialFunction[Module, Location]

  val BaseStrategy: Strategy = {
    case source: BridgeModule[_, _, _] =>
      Remote
    case task: GearpumpTaskModule =>
      Remote
    case groupBy: GroupByModule[_, _] =>
      // TODO: groupBy is not supported by local materializer
      Remote
    case source: SourceModule[_, _] =>
      Local
    case sink: SinkModule[_, _] =>
      Local
    case remaining: Module =>
      remaining.shape match {
        case sourceShape: SourceShape[_] =>
          Local
        case sinkShape: SinkShape[_] =>
          Local
        case otherShapes: Shape =>
          Remote
      }
  }

  val AllRemoteStrategy: Strategy = BaseStrategy orElse {
    case graphStageModule: GraphStageModule =>
      graphStageModule.stage match {
        case matValueSource: MaterializedValueSource[_] =>
          Local
        case singleSource: SingleSource[_] =>
          Local
        case _ =>
          Remote
      }
    case _ =>
      Remote
  }

  /**
   * Will decide whether to render a module locally or remotely
   * based on Attribute settings.
   *
   */
  val TagAttributeStrategy: Strategy = BaseStrategy orElse {
    case module =>
      GearAttributes.location(module.attributes)
  }

  val AllLocalStrategy: Strategy = BaseStrategy orElse {
    case graphStageModule: GraphStageModule =>
      // TODO kasravi review
      graphStageModule.stage match {
        case matValueSource: MaterializedValueSource[_] =>
          Local
        case _ =>
          Local
      }
    case _ =>
      Local
  }

  def apply(strategy: Strategy): GraphPartitioner = {
    new GraphPartitioner(strategy)
  }
}
