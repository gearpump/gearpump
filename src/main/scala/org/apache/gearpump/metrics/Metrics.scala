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
import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.agent.Agent
import akka.util.Timeout
import com.codahale.metrics.graphite.{GraphiteReporter, Graphite}
import com.codahale.metrics.{MetricFilter, ScheduledReporter, MetricRegistry}
import org.apache.gearpump.transport.netty.{TaskMessage, Context}
import org.slf4j.{LoggerFactory, Logger}

import scala.concurrent.Await

class Metrics(val system: ExtendedActorSystem) extends Extension {
import Metrics._

  private var sampleRate = 1
  lazy val metrics = {

    val metricsEnabled = system.settings.config.getBoolean("gearpump.metrics.enabled")

    LOG.info(s"Metrics is enabled...,  $metricsEnabled")



    var reporter : ScheduledReporter = null
    if (metricsEnabled) {

      val metrics = new MetricRegistry()

      val graphiteHost = system.settings.config.getString("gearpump.metrics.graphite.host")
      val graphitePort = system.settings.config.getInt("gearpump.metrics.graphite.port")

      sampleRate = system.settings.config.getInt("gearpump.metrics.sample.rate")

      val graphite = new Graphite(new InetSocketAddress(graphiteHost, graphitePort));

      val reporter = GraphiteReporter.forRegistry(metrics)
        .prefixedWith( s"host${system.provider.getDefaultAddress.host.get}".replace(".", "_"))
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .filter(MetricFilter.ALL)
        .build(graphite)

      LOG.info(s"Metrics is enabled..., reporting to $graphiteHost, $graphitePort")

      reporter.start(5, TimeUnit.SECONDS)

      system.registerOnTermination(new Runnable {
        override def run = reporter.stop()
      })

      metrics
    } else {
      null
    }
  }

  def meter(name : String) = {
    if (null == metrics) {
      new Meter(null)
    } else {
      new Meter(metrics.meter(name), sampleRate)
    }
  }

  def histogram(name : String) = {
    if (null == metrics) {
      new Histogram(null)
    } else {
      new Histogram(metrics.histogram(name), sampleRate)
    }
  }

  def counter(name : String) = {
    if (null == metrics) {
      new Counter(null)
    } else {
      new Counter(metrics.counter(name), sampleRate)
    }
  }
}

object Metrics extends ExtensionId[Metrics] with ExtensionIdProvider {
  val LOG: Logger = LoggerFactory.getLogger(classOf[Metrics])

  override def get(system: ActorSystem): Metrics = super.get(system)

  override def lookup = Metrics

  override def createExtension(system: ExtendedActorSystem): Metrics = new Metrics(system)
}