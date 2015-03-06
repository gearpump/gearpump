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

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import akka.actor._
import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import com.codahale.metrics.{Slf4jReporter, ConsoleReporter, MetricFilter, MetricRegistry}
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger
import upickle.Js

import scala.reflect.ClassTag

class Metrics(sampleRate: Int) extends Extension {

  val registry = new MetricRegistry()

  def meter(name : String) = {
    new Meter(name, registry.meter(name), sampleRate)
  }

  def histogram(name : String) = {
    new Histogram(name, registry.histogram(name), sampleRate)
  }

  def counter(name : String) = {
    new Counter(name, registry.counter(name), sampleRate)
  }
}

object Metrics extends ExtensionId[Metrics] with ExtensionIdProvider {

  val LOG: Logger = LogUtil.getLogger(getClass)
  import org.apache.gearpump.util.Constants._

  sealed trait MetricType {
    def name: String
  }

  object MetricType {

    implicit val metricJsonWriter: upickle.Writer[MetricType] = upickle.Writer[MetricType] {
      case histogram: Histogram => upickle.writeJs(histogram)
      case counter: Counter => upickle.writeJs(counter)
      case meter: Meter => upickle.writeJs(meter)
      case timer: Timer => upickle.writeJs(timer)
      case gauge: Gauge[_] =>
        upickle.writeJs(Map("name"-> gauge.name, "value" -> gauge.value.toString))
    }
  }

  case class Histogram
      (name: String, count: Long, min: Long, max: Long, mean: Double,
       stddev: Double, median: Double, p75: Double,
       p95: Double, p98: Double, p99: Double, p999: Double)
    extends MetricType

  case class Counter(name: String, value: Long) extends MetricType

  case class Meter(
      name: String, count: Long, meanRate: Double,
      m1: Double, m5: Double, m15: Double, rateUnit: String)
    extends MetricType

  case class Timer(
      name: String, count: Long, min: Double, max: Double,
      mean: Double, stddev: Double, median: Double,
      p75: Double, p95: Double, p98: Double,
      p99: Double, p999: Double, meanRate: Double,
      m1: Double, m5: Double, m15: Double,
      rateUnit: String, durationUnit: String)
    extends MetricType

  case class Gauge[T:ClassTag](name: String, value: T) extends MetricType

  override def get(system: ActorSystem): Metrics = super.get(system)

  override def lookup = Metrics

  override def createExtension(system: ExtendedActorSystem): Metrics = Metrics(system)

  def apply(system: ExtendedActorSystem): Metrics = {

    val metricsEnabled = system.settings.config.getBoolean(GEARPUMP_METRIC_ENABLED)
    LOG.info(s"Metrics is enabled...,  $metricsEnabled")
    val sampleRate = system.settings.config.getInt(GEARPUMP_METRIC_SAMPLE_RATE)
    val meters = new Metrics(sampleRate)

    if (metricsEnabled) {

      val reportInterval = system.settings.config.getInt(GEARPUMP_METRIC_REPORT_INTERVAL)

      def startGraphiteReporter = {
        val graphiteHost = system.settings.config.getString(GEARPUMP_METRIC_GRAPHITE_HOST)
        val graphitePort = system.settings.config.getInt(GEARPUMP_METRIC_GRAPHITE_PORT)

        val graphite = new Graphite(new InetSocketAddress(graphiteHost, graphitePort))

        val reporter = GraphiteReporter.forRegistry(meters.registry)
          .prefixedWith(s"host${system.provider.getDefaultAddress.host.get}".replace(".", "_"))
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS)
          .filter(MetricFilter.ALL)
          .build(graphite)

        LOG.info(s"reporting to $graphiteHost, $graphitePort")

        reporter.start(reportInterval, TimeUnit.MILLISECONDS)

        system.registerOnTermination(new Runnable {
          override def run = reporter.stop()
        })
      }

      def startSlf4jReporter = {

        val reporter = Slf4jReporter.forRegistry(meters.registry)
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS)
          .filter(MetricFilter.ALL)
          .outputTo(LOG)
          .build()

        reporter.start(reportInterval, TimeUnit.MILLISECONDS)

        system.registerOnTermination(new Runnable {
          override def run = reporter.stop()
        })
      }

      def startAkkaReporter = {

        val reporter = AkkaReporter.forRegistry(meters.registry)
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS)
          .filter(MetricFilter.ALL)
          .build(system)

        reporter.start(reportInterval, TimeUnit.MILLISECONDS)

        system.registerOnTermination(new Runnable {
          override def run = reporter.stop()
        })
      }

      val reporter = system.settings.config.getString(GEARPUMP_METRIC_REPORTER)

      LOG.info(s"Metrics reporter is enabled, using $reporter reporter")

      reporter match {
        case "graphite" => startGraphiteReporter
        case "logfile" => startSlf4jReporter
        case "akka" => startAkkaReporter
        case other =>
          LOG.error(s"Metrics reporter will be disabled, as we cannot recognize reporter: $other")
      }
    }

    meters
  }
}
