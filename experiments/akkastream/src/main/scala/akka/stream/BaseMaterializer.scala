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

package akka.stream

import scala.concurrent.ExecutionContextExecutor

/**
 * [[BaseMaterializer]] is a extension to [[akka.stream.Materializer]].
 *
 * Compared with [[akka.stream.Materializer]], the difference is that
 * [[materialize]] accepts a [[ModuleGraph]] instead of a RunnableGraph.
 *
 * @see [[ModuleGraph]] for the difference between RunnableGraph and
 * [[ModuleGraph]]
 *
 */
abstract class BaseMaterializer extends akka.stream.Materializer {

  override def withNamePrefix(name: String): Materializer = throw new UnsupportedOperationException()

  override implicit def executionContext: ExecutionContextExecutor = throw new UnsupportedOperationException()


  def materialize[Mat](graph: ModuleGraph[Mat]): Mat

  override def materialize[Mat](runnableGraph: Graph[ClosedShape, Mat]): Mat = {
    val graph = ModuleGraph(runnableGraph)
    materialize(graph)
  }

  def shutdown: Unit
}
