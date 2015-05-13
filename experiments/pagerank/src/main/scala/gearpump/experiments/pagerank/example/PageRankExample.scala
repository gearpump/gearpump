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
package gearpump.experiments.pagerank.example

import gearpump.experiments.pagerank.PageRankApplication
import gearpump.cluster.client.ClientContext
import gearpump.util.Graph
import gearpump.util.Graph.Node

object PageRankExample extends App {

  val a = "a"
  val b = "b"
  val c = "c"
  val d = "d"

  val pageRankGraph = Graph(a ~> b, a~>c, a~>d, b~>a, b~>d, d~>b, d~>c, c~>b)
  
  val app = new PageRankApplication("pagerank", iteration = 100, delta = 0.001, pageRankGraph)

  val context = ClientContext()

  val appId = context.submit(app)

  context.close()
}
