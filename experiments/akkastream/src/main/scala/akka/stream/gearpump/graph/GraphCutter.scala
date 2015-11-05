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

package akka.stream.gearpump.graph

import akka.stream.Attributes.Attribute
import akka.stream.ModuleGraph
import akka.stream.ModuleGraph.Edge
import akka.stream.gearpump.graph.GraphCutter.{Local, Remote, Strategy, Tag}
import akka.stream.gearpump.module.BridgeModule.{SinkBridgeModule, SourceBridgeModule}
import akka.stream.gearpump.module.{BridgeModule, DummyModule}
import akka.stream.impl.Stages.StageModule
import akka.stream.impl.StreamLayout.{MaterializedValueNode, Module}
import akka.stream.impl.{FanIn, FanOut, SinkModule, SourceModule}
import io.gearpump.util.Graph

/**
 *
 * GraphCutter is used to decide which part will be rendered locally
 * and which part should be rendered remotely. As Some part of the
 * Graph cannot be rendered remotely.
 *
 * For now, we will use simple strategy to cut the ModuleGraph,
 * All SourceModule, SinkModule will be sent to LocalGraph
 * All other module will be sent to RemoteGraph.
 *
 * TODO: In next version, we may want to cut the Graph based on
 * Attribute user configured.
 *
 */
class GraphCutter(strategy: Strategy = GraphCutter.AllRemoteStrategy) {
  def cut(moduleGraph: ModuleGraph[_]): List[SubGraph] = {
    val graph = removeDummyModule(moduleGraph.graph)
    val tags = tag(graph, strategy)
    doCut(graph, tags, moduleGraph.mat)
  }

  private def doCut(graph: Graph[Module, Edge], tags: Map[Module, Tag], mat: MaterializedValueNode): List[SubGraph] = {
    val local = Graph.empty[Module, Edge]
    val remote = Graph.empty[Module, Edge]

    graph.vertices.foreach{ module =>
      if (tags(module) == Local) {
        local.addVertex(module)
      } else {
        remote.addVertex(module)
      }
    }

    graph.edges.foreach{ nodeEdgeNode =>
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

  private def tag(graph: Graph[Module, Edge], strategy: Strategy): Map[Module, Tag] = {
    graph.vertices.map{vertex =>
      vertex match {
        case source: BridgeModule[_, _, _] =>
          vertex -> Remote
        case source: SourceModule[_, _] =>
          vertex -> Local
        case sink: SinkModule[_, _] =>
          vertex -> Local
        case other =>
          (other -> strategy(vertex))
      }
    }.toMap
  }

  private def removeDummyModule(inputGraph: Graph[Module, Edge]): Graph[Module, Edge] = {
    val graph = inputGraph.copy
    val dummies = graph.vertices.filter {module =>
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

object GraphCutter {
  sealed trait Tag
  object Local extends Tag
  object Remote extends Tag

  type Strategy = (Module => Tag)

  val AllRemoteStrategy: Strategy = { vertex: Module =>
    vertex match {
      case stage: StageModule =>
        Remote
      case fanIn: FanIn =>
        Remote
      case fanOut: FanOut =>
        Remote
    }
  }

  final case class TagAttribute(tag: Tag) extends Attribute

  val TagAttributeStrategy: Strategy = { vertex: Module =>
    vertex match {
      case other =>
        val tag = other.attributes.getAttribute(classOf[TagAttribute], TagAttribute(Local))
        tag.tag
    }
  }

  val AllLocalStrategy: Strategy = { vertex: Module =>
    vertex match {
      case other =>
        Local
    }
  }
}