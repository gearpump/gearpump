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

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import com.codahale.metrics._
import org.slf4j.Marker
import java.util.{SortedMap => JavaSortedMap}
import com.codahale.metrics.{Gauge => CodaGauge, Counter => CodaCounter, Histogram => CodaHistogram, Meter => CodaMeter, Timer => CodaTimer}
import scala.util.Try
import org.apache.gearpump.util.LogUtil

/**
 * A reporter class for logging metrics values to a remote actor periodically
 */
class AkkaReporter(
    system: ActorSystem,
    registry: MetricRegistry,
    marker: Marker,
    rateUnit:TimeUnit,
    durationUnit:TimeUnit,
    filter: MetricFilter)
  extends ScheduledReporter(registry, "akka-reporter", filter, rateUnit, durationUnit) {

  private val LOG = LogUtil.getLogger(getClass)
  LOG.info("Start Metrics AkkaReporter")

  override def report(
    gauges: JavaSortedMap[String, CodaGauge[_]],
    counters: JavaSortedMap[String, CodaCounter],
    histograms: JavaSortedMap[String, CodaHistogram],
    meters: JavaSortedMap[String, CodaMeter],
    timers: JavaSortedMap[String, CodaTimer]): Unit = {

    Try{

      import scala.collection.JavaConversions._

      //TODO: depends on Gauge refactory. @see Metrics.scala
//      val sgauges = collection.SortedMap(gauges.toSeq: _*)
//      sgauges.foreach(pair => {
//        import org.apache.gearpump.metrics.Metrics._
//        val (key, value) = pair
//        system.eventStream.publish(Gauge(key, value))
//      })

      val scounters = collection.SortedMap(counters.toSeq: _*)
      scounters.foreach(pair => {
        import org.apache.gearpump.metrics.Metrics._
        val (key, value: com.codahale.metrics.Counter) = pair
        system.eventStream.publish(Counter(key, value.getCount))
      })

      val shistograms = collection.SortedMap(histograms.toSeq: _*)
      shistograms.foreach(pair => {
        import org.apache.gearpump.metrics.Metrics._
        val (key, value: com.codahale.metrics.Histogram) = pair
        val s = value.getSnapshot
        system.eventStream.publish(
          Histogram(
            key, value.getCount, s.getMin, s.getMax, s.getMean,
            s.getStdDev, s.getMedian, s.get75thPercentile,
            s.get95thPercentile, s.get98thPercentile,
            s.get99thPercentile, s.get999thPercentile))
      })

      val smeters = collection.SortedMap(meters.toSeq: _*)
      smeters.foreach(pair => {
        import org.apache.gearpump.metrics.Metrics._
        val (key, value: com.codahale.metrics.Meter) = pair
        system.eventStream.publish(
          Meter(key,
            value.getCount,
            convertRate(value.getMeanRate),
            convertRate(value.getOneMinuteRate),
            convertRate(value.getFiveMinuteRate),
            convertRate(value.getFifteenMinuteRate),
            getRateUnit))
      })

      val stimers = collection.SortedMap(timers.toSeq: _*)
      stimers.foreach(pair => {
        import org.apache.gearpump.metrics.Metrics._
        val (key, value: com.codahale.metrics.Timer) = pair
        val s = value.getSnapshot
        system.eventStream.publish(
          Timer(key,
            value.getCount,
            convertDuration(s.getMin),
            convertDuration(s.getMax),
            convertDuration(s.getMean),
            convertDuration(s.getStdDev),
            convertDuration(s.getMedian),
            convertDuration(s.get75thPercentile),
            convertDuration(s.get95thPercentile),
            convertDuration(s.get98thPercentile),
            convertDuration(s.get99thPercentile),
            convertDuration(s.get999thPercentile),
            convertRate(value.getMeanRate),
            convertRate(value.getOneMinuteRate),
            convertRate(value.getFiveMinuteRate),
            convertRate(value.getFifteenMinuteRate),
            getRateUnit,
            getDurationUnit))
      })
    }.failed.map { ex =>
      LOG.error("failed to report metrics", ex)
    }
  }

  override def getRateUnit: String = {
    "events/" + super.getRateUnit
  }
}

object AkkaReporter {
  case class Builder(
      registry: MetricRegistry,
      marker: Marker,
      var rateUnit: TimeUnit,
      var durationUnit: TimeUnit,
      var flter: MetricFilter) {

    def build(system: ActorSystem) = {
      new AkkaReporter(system, registry, marker, rateUnit, durationUnit, flter)
    }

    def convertRatesTo(ru: TimeUnit) = {
      this.rateUnit = ru
      this
    }

    def convertDurationsTo(du: TimeUnit) = {
      this.durationUnit = du
      this
    }

    def filter(f: MetricFilter) = {
      this.flter = f
      this
    }
  }

  object Builder {
    def apply(registry: MetricRegistry): Builder = {
      Builder(registry, null, TimeUnit.SECONDS, TimeUnit.MILLISECONDS, MetricFilter.ALL)
    }
  }
  def forRegistry(registry: MetricRegistry): Builder = {
    Builder(registry)
  }
}