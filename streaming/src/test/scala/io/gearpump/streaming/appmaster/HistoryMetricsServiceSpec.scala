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

package io.gearpump.streaming.appmaster

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestProbe
import io.gearpump.cluster.ClientToMaster.QueryHistoryMetrics
import io.gearpump.cluster.MasterToClient.HistoryMetrics
import io.gearpump.cluster.TestUtil
import io.gearpump.metrics.Metrics.{Counter, Histogram, Meter}
import io.gearpump.util.HistoryMetricsService
import io.gearpump.util.HistoryMetricsService._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import scala.concurrent.Await

class HistoryMetricsServiceSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  val count = 2
  val intervalMs = 10

  val config = HistoryMetricsConfig(
    retainHistoryDataHours = 72,
    retainHistoryDataIntervalMs = 3600 * 1000,
    retainRecentDataSeconds = 300,
    retainRecentDataIntervalMs = 15 * 1000)

  "SingleValueMetricsStore" should "retain metrics and expire old value" in {

    val store = new SingleValueMetricsStore(count, intervalMs)

    var now = 0L
    // Only 1 data point will be kept in @intervalMs
    store.add(Counter("count", 1), now)
    store.add(Counter("count", 2), now)

    now = now + intervalMs + 1

    // Only 1 data point will be kept in @intervalMs
    store.add(Counter("count", 3), now)
    store.add(Counter("count", 4), now)

    now = now + intervalMs + 1

    // Only 1 data point will be kept in @intervalMs
    // expire oldest data point, because we only keep @count records
    store.add(Counter("count", 5), now)
    store.add(Counter("count", 6), now)

    val result = store.read
    assert(result.size == count)

    // The oldest value is expired
    assert(result.head.value.asInstanceOf[Counter].value == 3L)

    // The newest value is inserted
    assert(result.last.value.asInstanceOf[Counter].value == 5L)
  }

  val meterTemplate = Meter("meter", 0, 0, 0, "s")

  "HistogramMetricsStore" should "retain corse-grain history and fine-grain recent data" in {
    val store = new HistogramMetricsStore(config)
    val a = Histogram(null, 100, 0, 0, 0, 0, 0)
    val b = Histogram(null, 200, 0, 0, 0, 0, 0)
    val c = Histogram(null, 50, 0, 0, 0, 0, 0)

    store.add(a)
    store.add(b)
    store.add(c)

    assert(store.readLatest.map(_.value) == List(c))
    assert(store.readRecent.map(_.value) == List(a))
    assert(store.readHistory.map(_.value) == List(a))
  }

  "MeterMetricsStore" should "retain corse-grain history and fine-grain recent data" in {
    val store = new MeterMetricsStore(config)

    val a = Meter(null, 1, 100, 0, null)
    val b = Meter(null, 1, 200, 0, null)
    val c = Meter(null, 1, 50, 0, null)

    store.add(a)
    store.add(b)
    store.add(c)

    assert(store.readLatest.map(_.value) == List(c))
    assert(store.readRecent.map(_.value) == List(a))
    assert(store.readHistory.map(_.value) == List(a))
  }

  "CounterMetricsStore" should "retain corse-grain history and fine-grain recent data" in {
    val store = new CounterMetricsStore(config)
    val a = Counter(null, 50)
    val b = Counter(null, 100)
    val c = Counter(null, 150)

    store.add(a)
    store.add(b)
    store.add(c)

    assert(store.readLatest.map(_.value) == List(c))
    assert(store.readRecent.map(_.value) == List(a))
    assert(store.readHistory.map(_.value) == List(a))
  }

  "HistoryMetricsService" should
    "retain lastest metrics data and allow user to query metrics by path" in {
    implicit val system = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
    val service = system.actorOf(Props(new HistoryMetricsService("app0", config)))
    service ! Counter("metric.counter", 0)
    service ! Meter("metric.meter", 0, 0, 0, null)
    service ! Histogram("metric.histogram", 0, 0, 0, 0, 0, 0)

    val client = TestProbe()

    // Filters metrics with path "metric.counter"
    client.send(service, QueryHistoryMetrics("metric.counter"))
    import scala.concurrent.duration._
    client.expectMsgPF(3.seconds) {
      case history: HistoryMetrics =>
        assert(history.path == "metric.counter")
        val metricList = history.metrics
        metricList.foreach(metricItem =>
          assert(metricItem.value.isInstanceOf[Counter])
        )
    }

    // Filters metrics with path "metric.meter"
    client.send(service, QueryHistoryMetrics("metric.meter"))
    client.expectMsgPF(3.seconds) {
      case history: HistoryMetrics =>
        assert(history.path == "metric.meter")
        val metricList = history.metrics
        metricList.foreach(metricItem =>
          assert(metricItem.value.isInstanceOf[Meter])
        )
    }

    // Filters metrics with path "metric.histogram"
    client.send(service, QueryHistoryMetrics("metric.histogram"))
    client.expectMsgPF(3.seconds) {
      case history: HistoryMetrics =>
        assert(history.path == "metric.histogram")
        val metricList = history.metrics
        metricList.foreach(metricItem =>
          assert(metricItem.value.isInstanceOf[Histogram])
        )
    }

    // Filters metrics with path prefix "metric", all metrics which can
    // match the path prefix will be retained.
    client.send(service, QueryHistoryMetrics("metric"))
    client.expectMsgPF(3.seconds) {
      case history: HistoryMetrics =>
        val metricList = history.metrics

        var counterFound = false
        var meterFound = false
        var histogramFound = false

        metricList.foreach(metricItem =>
          metricItem.value match {
            case _: Counter => counterFound = true
            case _: Meter => meterFound = true
            case _: Histogram => histogramFound = true
            case _ => // Skip
          }
        )

        // All kinds of metric type are reserved.
        assert(counterFound && meterFound && histogramFound)
    }

    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }
}
