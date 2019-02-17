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
package io.gearpump.streaming.task

import io.gearpump.streaming.{DAG, ProcessorDescription}
import io.gearpump.streaming.partitioner.{HashPartitioner, Partitioner}
import io.gearpump.streaming.task.SubscriberSpec.TestTask
import io.gearpump.util.Graph
import io.gearpump.util.Graph._
import org.scalatest.{FlatSpec, Matchers}

class SubscriberSpec extends FlatSpec with Matchers {
  "Subscriber.of" should "return all subscriber for a processor" in {

    val sourceProcessorId = 0
    val task1 = ProcessorDescription(id = sourceProcessorId, taskClass =
      classOf[TestTask].getName, parallelism = 1)
    val task2 = ProcessorDescription(id = 1, taskClass = classOf[TestTask].getName, parallelism = 1)
    val task3 = ProcessorDescription(id = 2, taskClass = classOf[TestTask].getName, parallelism = 1)
    val partitioner = Partitioner[HashPartitioner]
    val dag = DAG(Graph(task1 ~ partitioner ~> task2, task1 ~ partitioner ~> task3,
      task2 ~ partitioner ~> task3))

    val subscribers = Subscriber.of(sourceProcessorId, dag)
    assert(subscribers.size == 2)

    assert(subscribers.toSet ==
      Set(Subscriber(1, partitioner, task2.parallelism, task2.life), Subscriber(2, partitioner,
        task3.parallelism, task3.life)))
  }
}

object SubscriberSpec {
  class TestTask
}
