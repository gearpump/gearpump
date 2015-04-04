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

package org.apache.gearpump.streaming.dsl

import akka.actor.ActorSystem
import org.apache.gearpump.cluster.{AppDescription, UserConfig}
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.streaming.{StreamApplication}
import org.apache.gearpump.streaming.dsl.op.{Op, OpEdge, InMemoryCollectionSource}
import org.apache.gearpump.streaming.dsl.plan.Planner
import org.apache.gearpump.util.Graph

/**
 * Example:
 *
 *
    val data = "This is a good start, bingo!! bingo!!"
    app.fromCollection(data.lines.toList).
      // word => (word, count)
      flatMap(line => line.split("[\\s]+")).map((_, 1)).
      // (word, count1), (word, count2) => (word, count1 + count2)
      groupBy(kv => kv._1).reduce(sum(_, _))

    val appId = context.submit(app)
    context.close()
 *
 * @param name
 */
class StreamApp(val name: String, context: ClientContext) {

  val graph = Graph.empty[Op, OpEdge]

  /**
   * Create a new stream from a in-memory collection.
   * This stream will repeat the source collection forever.
   *
   * @param collection
   * @param parallism
   * @tparam T
   * @return
   */
  def fromCollection[T](collection: List[T], parallism: Int = 1, description: String = null): Stream[T] = {
    val source = InMemoryCollectionSource(collection, parallism, Option(description).getOrElse("list"))
    graph.addVertex(source)
    new Stream[T](graph, source)
  }

  def plan: StreamApplication = {
    implicit val system = context.system
    val planner = new Planner
    val dag = planner.plan(graph)
    StreamApplication(name, dag, UserConfig.empty)
  }

  private[StreamApp] def system: ActorSystem = context.system
}

object StreamApp {

  implicit def streamAppToApplication(streamApp: StreamApp): StreamApplication = {
    streamApp.plan
  }
}