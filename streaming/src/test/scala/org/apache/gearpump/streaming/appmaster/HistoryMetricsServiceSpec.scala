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

package org.apache.gearpump.streaming.appmaster

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestProbe
import org.apache.gearpump.cluster.ClientToMaster.QueryHistoryMetrics
import org.apache.gearpump.cluster.TestUtil
import org.apache.gearpump.shared.Messages._
import org.apache.gearpump.streaming.appmaster.HistoryMetricsService._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class HistoryMetricsServiceSpec  extends FlatSpec with Matchers with BeforeAndAfterEach {

  val count = 2
  val intervalMs = 10

  val config = HistoryMetricsConfig(
    retainHistoryDataHours = 72,
    retainHistoryDataIntervalMs = 3600 * 1000,
    retainRecentDataSeconds = 300,
    retainRecentDataIntervalMs = 15 * 1000)

  "SingleValueMetricsStore" should "retain metrics and expire old value" in {

    val store = new SingleValueMetricsStore(count, intervalMs)

    //only 1 data point will be kept in @intervalMs
    store.add(Counter("count", 1))
    store.add(Counter("count", 2))

    //sleep @intervalMs + 1 so that we are allowed to push new data
    Thread.sleep(intervalMs + 1)

    //only 1 data point will be kept in @intervalMs
    store.add(Counter("count", 3))
    store.add(Counter("count", 4))

    //sleep @intervalMs + 1 so that we are allowed to push new data
    Thread.sleep(intervalMs + 1)

    //only 1 data point will be kept in @intervalMs
    //expire oldest data point, because we only keep @count records
    store.add(Counter("count", 5))
    store.add(Counter("count", 6))

    val result = store.read
    assert(result.size == count)

    //the oldest value is expired
    assert(upickle.read[Counter](result.head.value.json).value == 3L)

    //the newest value is inserted
    assert(upickle.read[Counter](result.last.value.json).value == 5L)
  }

  val meterTemplate = Meter("meter", 0, 0, 0, 0, 0, "s")

  "MinMaxMetricsStore" should "retain min and max metrics data and expire old value" in {

    val compartor = (left: HistoryMetricsItem, right: HistoryMetricsItem) =>
      upickle.read[Meter](left.value.json).meanRate > upickle.read[Meter](right.value.json).meanRate

    val store = new MinMaxMetricsStore(count, intervalMs, compartor)

    val min = 1
    val max = 10

    // only two data points will be kept in @intervalMs, one is the min, the other is the max
    (min to max).foreach(num => store.add(meterTemplate.copy(name = "A", meanRate = num)))

    //sleep @intervalMs + 1 so that we are allowed to push new data
    Thread.sleep(intervalMs + 1)

    // only two data points will be kept in @intervalMs, one is the min, the other is the max
    (min to max).foreach(num => store.add(meterTemplate.copy(name = "B", meanRate = num)))

    //sleep @intervalMs + 1 so that we are allowed to push new data
    Thread.sleep(intervalMs + 1)

    // only two data points will be kept in @intervalMs, one is the min, the other is the max
    // also will expire the oldest data, because we only keep @count records
    (min to max).foreach(num => store.add(meterTemplate.copy(name = "C", meanRate = num)))

    val result = store.read
    assert(result.size == count * 2)

    //the oldest value A is expired
    assert(upickle.read[Meter](result.head.value.json).name == "B")
    //the first value is "B", min, the second first value is "B", "max"
    assert(upickle.read[Meter](result.head.value.json).meanRate == min)

    //the newest value C is expired
    assert(upickle.read[Meter](result.last.value.json).name == "C")
    //the last value is "C", max, the second last value is "C", "min"
    assert(upickle.read[Meter](result.last.value.json).meanRate == max)

  }

  "HistogramMetricsStore" should "retain corse-grain history and fine-grain recent data" in {
    val store = new HistogramMetricsStore(config)
    store.add(Histogram(null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))

    //there should be 3 records, min-history, max-history, and recent
    assert(store.read.size == 3)
  }

  "MeterMetricsStore" should "retain corse-grain history and fine-grain recent data" in {
    val store = new MeterMetricsStore(config)
    store.add(Meter(null, 0, 0, 0, 0, 0, null))

    //there should be 3 records, min-history, max-history, and recent
    assert(store.read.size == 3)
  }

  "CounterMetricsStore" should "retain corse-grain history and fine-grain recent data" in {
    val store = new CounterMetricsStore(config)
    store.add(Counter(null, 0))

    //there should be 2 records, history, and recent
    assert(store.read.size == 2)
  }

  "HistoryMetricsService" should "retain lastest metrics data and allow user to query metrics by path" in {
    implicit val system = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
    val appId = 0
    val service = system.actorOf(Props(new HistoryMetricsService(0, config)))
    service ! Counter("metric.counter", 0)
    service ! Meter("metric.meter", 0, 0, 0, 0, 0, null)
    service ! Histogram("metric.histogram", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    val client = TestProbe()

    // filter metrics with path "metric.counter"
    client.send(service, QueryHistoryMetrics(appId, "metric.counter"))
    import scala.concurrent.duration._
    client.expectMsgPF(3 seconds) {
      case history: HistoryMetrics =>
        assert(history.appId == appId)
        assert(history.path == "metric.counter")
        val metricList = history.metrics
        metricList.foreach(metricItem =>
          assert(upickle.read[Counter](metricItem.value.json).isInstanceOf[Counter])
        )
    }

    // filter metrics with path "metric.meter"
    client.send(service, QueryHistoryMetrics(appId, "metric.meter"))
    client.expectMsgPF(3 seconds) {
      case history: HistoryMetrics =>
        assert(history.appId == appId)
        assert(history.path == "metric.meter")
        val metricList = history.metrics
        metricList.foreach(metricItem =>
          assert(upickle.read[Meter](metricItem.value.json).isInstanceOf[Meter])
        )
    }

    // filter metrics with path "metric.histogram"
    client.send(service, QueryHistoryMetrics(appId, "metric.histogram"))
    client.expectMsgPF(3 seconds) {
      case history: HistoryMetrics =>
        assert(history.appId == appId)
        assert(history.path == "metric.histogram")
        val metricList = history.metrics
        metricList.foreach(metricItem =>
          assert(upickle.read[Histogram](metricItem.value.json).isInstanceOf[Histogram])
        )
    }

    val CounterType = Counter.getClass.getCanonicalName.substring(0, Counter.getClass.getCanonicalName.length-1)
    val MeterType = Meter.getClass.getCanonicalName.substring(0, Meter.getClass.getCanonicalName.length-1)
    val HistogramType = Histogram.getClass.getCanonicalName.substring(0, Histogram.getClass.getCanonicalName.length-1)

    // filter metrics with path prefix "metric", all metrics which can
    // match the path prefix will be retained.
    client.send(service, QueryHistoryMetrics(appId, "metric"))
    client.expectMsgPF(3 seconds) {
      case history: HistoryMetrics =>
        val metricList = history.metrics

        var counterFound = false
        var meterFound = false
        var histogramFound = false

        metricList.foreach(metricItem =>
          metricItem.value.typeName match {
            case CounterType =>
              upickle.read[Counter](metricItem.value.json)
              counterFound = true
            case MeterType =>
              upickle.read[Meter](metricItem.value.json)
              meterFound = true
            case HistogramType =>
              upickle.read[Histogram](metricItem.value.json)
              histogramFound = true
            case _ =>
          }
        )

        // All kinds of metric type are reserved.
        assert(counterFound && meterFound && histogramFound)
    }

    system.shutdown()

  }
}
