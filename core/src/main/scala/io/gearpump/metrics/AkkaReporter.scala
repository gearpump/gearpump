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

package io.gearpump.metrics

import akka.actor.ActorRef
import com.codahale.metrics.{Gauge => CodaGauge, MetricRegistry}
import io.gearpump.metrics.Metrics.{Counter => CounterData, Gauge => GaugeData, Histogram => HistogramData, Meter => MeterData}
import io.gearpump.metrics.MetricsReporterService.ReportTo
import io.gearpump.util.LogUtil
import scala.collection.JavaConverters._

/**
 * A reporter class for logging metrics values to a remote actor periodically
 */
class AkkaReporter(registry: MetricRegistry)
  extends ReportTo {
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
        key, s.getMean, s.getStdDev, s.getMedian,
        s.get95thPercentile, s.get99thPercentile, s.get999thPercentile)
    }

    meters.entrySet().asScala.foreach { pair =>
      val key = pair.getKey
      val value = pair.getValue
      to ! MeterData(key,
        value.getCount,
        value.getMeanRate,
        value.getOneMinuteRate,
        getRateUnit)
    }

    gauges.entrySet().asScala.foreach { kv =>
      val value = kv.getValue.asInstanceOf[CodaGauge[Number]].getValue.longValue()
      to ! GaugeData(kv.getKey, value)
    }
  }

  private def getRateUnit: String = {
    "events/s"
  }
}