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

import com.github.ghik.silencer.silent
import io.gearpump.cluster.ClientToMaster.ReadOption
import io.gearpump.cluster.MasterToClient.HistoryMetricsItem
import io.gearpump.metrics.Metrics.{Gauge, Histogram, Meter}
import io.gearpump.streaming.metrics.ProcessorAggregator.{AggregatorFactory, HistogramAggregator, MeterAggregator, MultiLayerMap}
import io.gearpump.streaming.task.TaskId
import io.gearpump.util.HistoryMetricsService.HistoryMetricsConfig
import org.scalatest.{FlatSpec, Matchers}
import scala.collection.JavaConverters._
import scala.util.Random

class ProcessorAggregatorSpec extends FlatSpec with Matchers {

  "MultiLayerMap" should "maintain multiple layers HashMap" in {
    val layers = 3
    val map = new MultiLayerMap[String](layers)

    assert(map.get(layer = 0, "key") == null)

    // Illegal, handle safely
    assert(map.get(layer = 10, "key") == null)

    map.put(layer = 0, "key", "value")
    assert(map.get(layer = 0, "key") == "value")

    map.put(layer = 1, "key2", "value2")
    map.put(layer = 2, "key3", "value3")
    map.put(layer = 2, "key4", "value4")

    // Illegal, should be ignored
    map.put(layer = 4, "key5", "value5")

    assert(map.size == 4)
    assert(map.valueIterator.asScala.toSet == Set("value", "value2", "value3", "value4"))
  }

  "HistogramAggregator" should "aggregate by calculating average" in {
    val aggregator = new HistogramAggregator("processor")

    val a = Histogram("processor.task1", 1, 2, 3, 4, 5, 6)
    val b = Histogram("processor.task2", 5, 6, 7, 8, 9, 10)
    val expect = Histogram("processor.task2", 3, 4, 5, 6, 7, 8)

    val olderTime = 100
    val newerTime = 200

    aggregator.aggregate(HistoryMetricsItem(time = newerTime, a))
    aggregator.aggregate(HistoryMetricsItem(time = olderTime, b))

    val result = aggregator.result

    // Picks old time as aggregated time
    assert(result.time == olderTime)

    // Does average
    val check = result.value.asInstanceOf[Histogram]

    assert(check.mean - expect.mean < 0.01)
    assert(check.stddev - expect.stddev < 0.01)
    assert(check.median - expect.median < 0.01)
    assert(check.p95 - expect.p95 < 0.01)
    assert(check.p99 - expect.p99 < 0.01)
    assert(check.p999 - expect.p999 < 0.01)
  }

  "MeterAggregator" should "aggregate by summing" in {
    val aggregator = new MeterAggregator("processor")

    val a = Meter("processor.task1", count = 1, 1, 3, "s")
    val b = Meter("processor.task2", count = 2, 5, 7, "s")
    val expect = Meter("processor.task2", count = 3, 6, 10, "s")

    val olderTime = 100
    val newerTime = 200

    aggregator.aggregate(HistoryMetricsItem(time = newerTime, a))
    aggregator.aggregate(HistoryMetricsItem(time = olderTime, b))

    val result = aggregator.result

    // Picks old time
    assert(result.time == olderTime)

    // Does summing
    val check = result.value.asInstanceOf[Meter]

    assert(check.count == expect.count)
    assert(check.m1 - expect.m1 < 0.01)
    assert(check.meanRate - expect.meanRate < 0.01)
    assert(check.rateUnit == expect.rateUnit)
  }

  "AggregatorFactory" should "create aggregator" in {
    val factory = new AggregatorFactory()

    val a = Meter("processor.task1", count = 1, 1, 3, "s")
    val b = Histogram("processor.task1", 1, 2, 3, 4, 5, 6)

    val aggegator1 = factory.create(HistoryMetricsItem(time = 0, a), "name1")
    assert(aggegator1.isInstanceOf[MeterAggregator])

    val aggegator2 = factory.create(HistoryMetricsItem(time = 0, b), "name2")
    assert(aggegator2.isInstanceOf[HistogramAggregator])
  }

  "ProcessorAggregator" should "aggregate on different read options" in {
    val hours = 2 // Maintains 2 hours history
    val seconds = 2 // Maintains 2 seconds recent data
    val taskCount = 5 // For each processor
    val metricCount = 100 // For each task, have metricCount metrics
    val range = new HistoryMetricsConfig(hours, hours / 2 * 3600 * 1000,
      seconds, seconds / 2 * 1000)

    val aggregator = new ProcessorAggregator(range)

    def count(value: Int): Int = value

    def inputs(timeRange: Long): List[HistoryMetricsItem] = {
      (0 until taskCount).map(TaskId(processorId = 0, _))
        .flatMap(histogram(_, "receiveLatency", timeRange, metricCount)).toList ++
        (0 until taskCount).map(TaskId(processorId = 0, _))
          .flatMap(histogram(_, "processTime", timeRange, metricCount)).toList ++
        (0 until taskCount).map(TaskId(processorId = 1, _))
          .flatMap(histogram(_, "receiveLatency", timeRange, metricCount)).toList ++
        (0 until taskCount).map(TaskId(processorId = 1, _))
          .flatMap(histogram(_, "processTime", timeRange, metricCount)).toList ++
        (0 until taskCount).map(TaskId(processorId = 0, _))
          .flatMap(meter(_, "sendThroughput", timeRange, metricCount)).toList ++
        (0 until taskCount).map(TaskId(processorId = 0, _))
          .flatMap(meter(_, "receiveThroughput", timeRange, metricCount)).toList ++
        (0 until taskCount).map(TaskId(processorId = 1, _))
          .flatMap(meter(_, "sendThroughput", timeRange, metricCount)).toList ++
        (0 until taskCount).map(TaskId(processorId = 1, _))
          .flatMap(meter(_, "receiveThroughput", timeRange, metricCount)).toList
    }

    def check(list: List[HistoryMetricsItem], countMap: Map[String, Int]): Boolean = {
      val nameCount = list.map(_.value.name).groupBy(key => key).mapValues(_.size).toList.toMap
      nameCount sameElements countMap
    }

    // Aggregates on processor and meterNames,
    val input = inputs(Long.MaxValue)
    val readLatest = aggregator.aggregate(ReadOption.ReadLatest,
      input.iterator, now = Long.MaxValue)
    assert(readLatest.size == 8) // 2 processor * 4 metrics type
    assert(check(readLatest, Map(
      "app0.processor0:receiveLatency" -> count(1),
      "app0.processor0:processTime" -> count(1),
      "app0.processor0:sendThroughput" -> count(1),
      "app0.processor0:receiveThroughput" -> count(1),
      "app0.processor1:receiveLatency" -> count(1),
      "app0.processor1:processTime" -> count(1),
      "app0.processor1:sendThroughput" -> count(1),
      "app0.processor1:receiveThroughput" -> count(1)
    )))

    // Aggregates on processor and meterNames and time range,
    val readRecent = aggregator.aggregate(ReadOption.ReadRecent,
      inputs(seconds * 1000).iterator, now = seconds * 1000)
    assert(readRecent.size == 16) // 2 processor * 4 metrics type * 2 time range
    assert(check(readRecent, Map(
      "app0.processor0:receiveLatency" -> count(2),
      "app0.processor0:processTime" -> count(2),
      "app0.processor0:sendThroughput" -> count(2),
      "app0.processor0:receiveThroughput" -> count(2),
      "app0.processor1:receiveLatency" -> count(2),
      "app0.processor1:processTime" -> count(2),
      "app0.processor1:sendThroughput" -> count(2),
      "app0.processor1:receiveThroughput" -> count(2)
    )))

    // Aggregates on processor and meterNames and time range,
    val readHistory = aggregator.aggregate(ReadOption.ReadHistory,
      inputs(hours * 3600 * 1000).iterator, now = hours * 3600 * 1000)
    assert(readHistory.size == 16) // 2 processor * 4 metrics type * 2 time ranges
    assert(check(readHistory, Map(
      "app0.processor0:receiveLatency" -> count(2),
      "app0.processor0:processTime" -> count(2),
      "app0.processor0:sendThroughput" -> count(2),
      "app0.processor0:receiveThroughput" -> count(2),
      "app0.processor1:receiveLatency" -> count(2),
      "app0.processor1:processTime" -> count(2),
      "app0.processor1:sendThroughput" -> count(2),
      "app0.processor1:receiveThroughput" -> count(2)
    )))
  }

  @silent // https://github.com/scala/bug/issues/7707
  private def histogram(
      taskId: TaskId, metricName: String = "latency", timeRange: Long = Long.MaxValue,
      repeat: Int = 1): List[HistoryMetricsItem] = {
    val random = new Random()
    (0 until repeat).map { _ =>
      new HistoryMetricsItem(Math.abs(random.nextLong() % timeRange),
        new Histogram(s"app0.processor${taskId.processorId}.task${taskId.index}:$metricName",
          Math.abs(random.nextDouble()),
          Math.abs(random.nextDouble()),
          Math.abs(random.nextDouble()),
          Math.abs(random.nextDouble()),
          Math.abs(random.nextDouble()),
          Math.abs(random.nextDouble())
        ))
    }.toList
  }

  private def meter(taskId: TaskId, metricName: String, timeRange: Long, repeat: Int)
    : List[HistoryMetricsItem] = {
    val random = new Random()
    (0 until repeat).map { _ =>
      new HistoryMetricsItem(Math.abs(random.nextLong() % timeRange),
        new Meter(s"app0.processor${taskId.processorId}.task${taskId.index}:$metricName",
          Math.abs(random.nextInt()),
          Math.abs(random.nextDouble()),
          Math.abs(random.nextDouble()),
          "event/s")
      )
    }.toList
  }

  "ProcessorAggregator" should "handle smoothly for unsupported metric type and " +
    "error formatted metric name" in {
    val invalid = List(
      // Unsupported metric type
      HistoryMetricsItem(0, new Gauge("app0.processor0.task0:gauge", 100)),

      // Wrong format: should be app0.processor0.task0:throughput
      HistoryMetricsItem(0, new Meter("app0.processor0.task0/throughput", 100, 0, 0, ""))
    )

    val valid = histogram(TaskId(0, 0), repeat = 10)

    val aggregator = new ProcessorAggregator(new HistoryMetricsConfig(0, 0, 0, 0))
    val result = aggregator.aggregate(ReadOption.ReadLatest, (valid ++ invalid).toIterator,
      now = Long.MaxValue)

    // For one taskId, will only use one data point.
    assert(result.size == 1)
    assert(result.head.value.name == "app0.processor0:latency")
  }
}
