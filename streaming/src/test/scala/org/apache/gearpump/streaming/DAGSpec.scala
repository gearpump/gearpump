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

package org.apache.gearpump.streaming

import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.util.Graph
import org.apache.gearpump.util.Graph._
import org.scalacheck.Gen
import org.scalatest.{PropSpec, Matchers}
import org.scalatest.prop.PropertyChecks

class DAGSpec extends PropSpec with PropertyChecks with Matchers {

  val parallelismGen = Gen.chooseNum[Int](1, 100)

  property("DAG should be built correctly for a single task") {
    forAll(parallelismGen) { (parallelism: Int) =>
      val task = TaskDescription("task", parallelism)
      val graph: Graph[TaskDescription, Partitioner] = Graph(task)
      val dag = DAG(graph)
      dag.processors.size shouldBe 1
      dag.graph.edges shouldBe empty
      val subGraph = dag.subGraph(0)
      subGraph.graph.vertices shouldBe Graph(0).vertices
      subGraph.processors(0).parallelism shouldBe parallelism
    }
  }
}
