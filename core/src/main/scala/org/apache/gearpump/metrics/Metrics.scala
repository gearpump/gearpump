/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package org.apache.gearpump.metrics

import scala.collection.JavaConverters._

import akka.actor._
import org.slf4j.Logger

import org.apache.gearpump.codahale.metrics._
import org.apache.gearpump.metrics
import org.apache.gearpump.util.LogUtil

/** Metric objects registry */
class Metrics(sampleRate: Int) extends Extension {

  val registry = new MetricRegistry()

  def meter(name: String): metrics.Meter = {
    new metrics.Meter(name, registry.meter(name), sampleRate)
  }

  def histogram(name: String): Histogram = {
    new Histogram(name, registry.histogram(name), sampleRate)
  }

  def histogram(name: String, sampleRate: Int): Histogram = {
    new Histogram(name, registry.histogram(name), sampleRate)
  }

  def counter(name: String): Counter = {
    new Counter(name, registry.counter(name), sampleRate)
  }

  def register(set: MetricSet): Unit = {
    val names = registry.getNames
    val metrics = set.getMetrics.asScala.filterKeys { key => !names.contains(key) }
    metrics.foreach { kv =>
      registry.register(kv._1, kv._2)
    }
  }
}

object Metrics extends ExtensionId[Metrics] with ExtensionIdProvider {

  val LOG: Logger = LogUtil.getLogger(getClass)
  import org.apache.gearpump.util.Constants._

  sealed trait MetricType {
    def name: String
  }

  object MetricType {
    def unapply(obj: MetricType): Option[(Histogram, Counter, Meter, Timer, Gauge)] = {
      obj match {
        case x: Histogram => Some((x, null, null, null, null))
        case x: Counter => Some((null, x, null, null, null))
        case x: Meter => Some((null, null, x, null, null))
        case x: Timer => Some((null, null, null, x, null))
        case g: Gauge => Some((null, null, null, null, g))
      }
    }

    def apply(h: Histogram, c: Counter, m: Meter, t: Timer, g: Gauge): MetricType = {
      val result =
        if (h != null) h
        else if (c != null) c
        else if (m != null) m
        else if (t != null) t
        else if (g != null) g
        else null
      result
    }
  }

  case class Histogram (
      name: String, mean: Double,
      stddev: Double, median: Double,
      p95: Double, p99: Double, p999: Double)
    extends MetricType

  case class Counter(name: String, value: Long) extends MetricType

  case class Meter(
      name: String, count: Long, meanRate: Double,
      m1: Double, rateUnit: String)
    extends MetricType

  case class Timer(
      name: String, count: Long, min: Double, max: Double,
      mean: Double, stddev: Double, median: Double,
      p75: Double, p95: Double, p98: Double,
      p99: Double, p999: Double, meanRate: Double,
      m1: Double, m5: Double, m15: Double,
      rateUnit: String, durationUnit: String)
    extends MetricType

  case class Gauge(name: String, value: Long) extends MetricType

  case object ReportMetrics

  case class DemandMoreMetrics(subscriber: ActorRef)

  override def get(system: ActorSystem): Metrics = super.get(system)

  override def lookup: ExtensionId[Metrics] = Metrics

  override def createExtension(system: ExtendedActorSystem): Metrics = {
    val metricsEnabled = system.settings.config.getBoolean(GEARPUMP_METRIC_ENABLED)
    LOG.info(s"Metrics is enabled...,  $metricsEnabled")
    val sampleRate = system.settings.config.getInt(GEARPUMP_METRIC_SAMPLE_RATE)
    if (metricsEnabled) {
      val meters = new Metrics(sampleRate)
      meters
    } else {
      new DummyMetrics
    }
  }

  class DummyMetrics extends Metrics(1) {
    override def register(set: MetricSet): Unit = Unit

    private val meter = new metrics.Meter("", null) {
      override def mark(): Unit = Unit
      override def mark(n: Long): Unit = Unit
      override def getOneMinuteRate(): Double = 0
    }

    private val histogram = new metrics.Histogram("", null) {
      override def update(value: Long): Unit = Unit
      override def getMean(): Double = 0
      override def getStdDev(): Double = 0
    }

    private val counter = new metrics.Counter("", null) {
      override def inc(): Unit = Unit
      override def inc(n: Long): Unit = Unit
    }

    override def meter(name: String): metrics.Meter = meter
    override def histogram(name: String): metrics.Histogram = histogram
    override def counter(name: String): metrics.Counter = counter
  }
}