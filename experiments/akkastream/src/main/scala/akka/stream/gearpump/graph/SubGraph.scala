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

import akka.actor.ActorSystem
import akka.stream.ModuleGraph.Edge
import akka.stream.impl.StreamLayout.Module
import io.gearpump.util.Graph

/**
 * [[SubGraph]] is a partial part of [[akka.stream.ModuleGraph]]
 *
 * The idea is that by dividing [[akka.stream.ModuleGraph]] to several
 * [[SubGraph]], we can materialize each [[SubGraph]] with different
 * materializer.
 */

trait SubGraph {

  /**
   * the [[Graph]] representation of this SubGraph
   * @return
   */
  def graph: Graph[Module, Edge]
}


/**
 * Materializer for Sub-Graph type
 */
trait SubGraphMaterializer{
  /**
   *
   * @param matValues Materialized Values for each module before materialization
   * @return Materialized Values for each Module after the materialization.
   */

  def materialize(graph: SubGraph, matValues: Map[Module, Any]): Map[Module, Any]

  def shutdown: Unit
}