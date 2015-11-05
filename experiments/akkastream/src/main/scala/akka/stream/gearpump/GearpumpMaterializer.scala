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

package akka.stream.gearpump

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.gearpump.graph.GraphCutter
import akka.stream.gearpump.graph.GraphCutter.Strategy
import akka.stream.impl.StreamLayout.Module

/**
 *
 * [[GearpumpMaterializer]] allow you to render akka-stream DSL as a Gearpump
 * streaming application. If some module cannot be rendered remotely in Gearpump
 * Cluster, then it will use local Actor materializer as fallback to materialize
 * the module locally.
 *
 * User can custom a [[Strategy]] to determinie which module should be rendered
 * remotely, and which module should be rendered locally.
 *
 * @see [[GraphCutter]] to find out how we cut the [[ModuleGraph]] to two parts,
 *   and materialize there seperately.
 *
 * @param system
 * @param strategy
 */
class GearpumpMaterializer(system: ActorSystem, strategy: Strategy = GraphCutter.AllRemoteStrategy)
    extends BaseMaterializer {

  override def materialize[Mat](graph: ModuleGraph[Mat]): Mat = {
    val subGraphs = new GraphCutter(strategy).cut(graph)
    val matValues = subGraphs.foldLeft(Map.empty[Module, Any]){(map, subGraph) =>
      map ++ subGraph.materialize(map, system)
    }
    graph.resolve(matValues)
  }
}

object GearpumpMaterializer{
  def apply(system: ActorSystem) = new GearpumpMaterializer(system)
}