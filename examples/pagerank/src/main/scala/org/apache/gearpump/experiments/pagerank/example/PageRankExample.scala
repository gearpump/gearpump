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
package org.apache.gearpump.experiments.pagerank.example

import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.experiments.pagerank.PageRankApplication
import org.apache.gearpump.util.Graph.Node
import org.apache.gearpump.util.{AkkaApp, Graph}

/** A very simple PageRank example, Cyclic graph is not supported */
object PageRankExample extends AkkaApp {

  val a = "a"
  val b = "b"
  val c = "c"
  val d = "d"

  def help(): Unit = Unit

  def main(akkaConf: Config, args: Array[String]): Unit = {
    val pageRankGraph = Graph(a ~> b, a ~> c, a ~> d, b ~> a, b ~> d, d ~> b, d ~> c, c ~> b)
    val app = new PageRankApplication("pagerank", iteration = 100, delta = 0.001, pageRankGraph)
    val context = ClientContext(akkaConf)
    val appId = context.submit(app)
    context.close()
  }
}
