/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.streaming

import io.gearpump.streaming.partitioner.PartitionerDescription
import io.gearpump.streaming.task.TaskId
import io.gearpump.util.Graph
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

class DAGSpec extends PropSpec with PropertyChecks with Matchers {

  val parallelismGen = Gen.chooseNum[Int](1, 100)

  property("DAG should be built correctly for a single task") {
    forAll(parallelismGen) { (parallelism: Int) =>
      val task = ProcessorDescription(id = 0, taskClass = "task", parallelism = parallelism)
      val graph = Graph[ProcessorDescription, PartitionerDescription](task)
      val dag = DAG(graph)
      dag.processors.size shouldBe 1
      assert(dag.taskCount == parallelism)
      dag.tasks.sortBy(_.index) shouldBe (0 until parallelism).map(index => TaskId(0, index))
      dag.graph.getEdges shouldBe empty
    }
  }
}
