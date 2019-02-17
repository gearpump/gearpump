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

package io.gearpump.streaming.metrics

import io.gearpump.cluster.MasterToClient.HistoryMetricsItem
import io.gearpump.metrics.Metrics.{Histogram, Meter}
import io.gearpump.streaming.metrics.TaskFilterAggregator.Options
import io.gearpump.streaming.task.TaskId
import org.scalatest.{FlatSpec, Matchers}
import scala.util.Random

class TaskFilterAggregatorSpec extends FlatSpec with Matchers {

  def metric(taskId: TaskId): HistoryMetricsItem = {
    val random = new Random()
    new HistoryMetricsItem(Math.abs(random.nextLong()),
      new Histogram(s"app0.processor${taskId.processorId}.task${taskId.index}:latency",
        0, 0, 0, 0, 0, 0))
  }

  it should "filter data on processor range, task range combination" in {
    val inputs = (0 until 10).flatMap { processor =>
      (0 until 10).map { task =>
        metric(TaskId(processor, task))
      }
    }.toList

    val globalLimit = 10
    val aggregator = new TaskFilterAggregator(globalLimit)

    // Limit not met, return all matches in this matrix
    var options = new Options(limit = 20, startTask = 3, endTask = 6,
      startProcessor = 3, endProcessor = 6)
    assert(aggregator.aggregate(options, inputs.iterator).size == 9)

    // User limit reached
    options = new Options(limit = 3, startTask = 3, endTask = 5,
      startProcessor = 3, endProcessor = 5)
    assert(aggregator.aggregate(options, inputs.iterator).size == 3)

    // Global limit reached
    options = new Options(limit = 20, startTask = 3, endTask = 8,
      startProcessor = 3, endProcessor = 8)
    assert(aggregator.aggregate(options, inputs.iterator).size == globalLimit)
  }

  it should "reject wrong format options" in {
    val options = Map(TaskFilterAggregator.StartTask -> "not a number")
    assert(Options.parse(options) == null)
  }

  it should "skip wrong format metrics" in {
    val invalid = List {
      // Wrong format: should be app0.processor0.task0:throughput
      HistoryMetricsItem(0, new Meter("app0.processor0.task0/throughput", 100, 0, 0, ""))
    }
    val options = Options.acceptAll

    val aggregator = new TaskFilterAggregator(Int.MaxValue)
    assert(aggregator.aggregate(options, invalid.iterator).size == 0)
  }
}
