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

package org.apache.gearpump.metrics

import akka.actor.{ActorRef, ActorSystem}
import org.apache.gearpump.metrics.Metrics.{Histogram => HistogramData, Meter => MeterData, Counter => CounterData, Gauge => GaugeData}
import io.gearpump.codahale.metrics.MetricRegistry

import akka.actor.ActorSystem
import org.slf4j.Marker
import java.util.{SortedMap => JavaSortedMap}
import io.gearpump.codahale.metrics.{Gauge => CodaGauge, Counter => CodaCounter, Histogram => CodaHistogram, Meter => CodaMeter, Timer => CodaTimer}
import scala.util.Try
import org.apache.gearpump.metrics.MetricsReporterService.ReportTo
import org.apache.gearpump.util.LogUtil
import scala.collection.JavaConverters._

/**
 * A reporter class for logging metrics values to a remote actor periodically
 */
class AkkaReporter(
    system: ActorSystem,
    registry: MetricRegistry)
  extends ReportTo{
  private val LOG = LogUtil.getLogger(getClass)
  LOG.info("Start Metrics AkkaReporter")

  override def report(to: ActorRef): Unit = {
    val counters = registry.getCounters()
    val histograms = registry.getHistograms()
    val meters = registry.getMeters()
    val gauges = registry.getGauges()

    counters.entrySet().asScala.foreach { pair =>
      to ! CounterData(pair.getKey, pair.getValue.getCount)
    }

    histograms.entrySet().asScala.foreach { pair =>
      val key = pair.getKey
      val value = pair.getValue
      val s = value.getSnapshot
      to ! HistogramData(
        key, value.getCount, s.getMin, s.getMax, s.getMean,
        s.getStdDev, s.getMedian, s.get75thPercentile,
        s.get95thPercentile, s.get98thPercentile,
        s.get99thPercentile, s.get999thPercentile)
    }

    meters.entrySet().asScala.foreach{pair =>
      val key = pair.getKey
      val value = pair.getValue
      to ! MeterData(key,
        value.getCount,
        value.getMeanRate,
        value.getOneMinuteRate,
        value.getFiveMinuteRate,
        value.getFifteenMinuteRate,
        getRateUnit)
    }

    gauges.entrySet().asScala.foreach {kv =>
      val value = kv.getValue.asInstanceOf[CodaGauge[Number]].getValue.longValue()
      to ! GaugeData(kv.getKey, value)
    }
  }

  private def getRateUnit: String = {
    "events/s"
  }
}