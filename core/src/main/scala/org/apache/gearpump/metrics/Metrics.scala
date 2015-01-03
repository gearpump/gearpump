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
import com.codahale.metrics.{MetricFilter, MetricRegistry, ScheduledReporter}
import org.apache.gearpump.util.LogUtil
import org.slf4j.{Logger, LoggerFactory}

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

  override def get(system: ActorSystem): Metrics = super.get(system)

  override def lookup = Metrics

  override def createExtension(system: ExtendedActorSystem): Metrics = Metrics(system)

  def apply(system: ExtendedActorSystem): Metrics = {

    val metricsEnabled = system.settings.config.getBoolean("gearpump.metrics.enabled")
    LOG.info(s"Metrics is enabled...,  $metricsEnabled")
    val sampleRate = system.settings.config.getInt("gearpump.metrics.sample.rate")
    val meters = new Metrics(sampleRate)

    if (metricsEnabled) {
      def startReporter = {
        val graphiteHost = system.settings.config.getString("gearpump.metrics.graphite.host")
        val graphitePort = system.settings.config.getInt("gearpump.metrics.graphite.port")

        val graphite = new Graphite(new InetSocketAddress(graphiteHost, graphitePort))

        val reporter = GraphiteReporter.forRegistry(meters.registry)
          .prefixedWith(s"host${system.provider.getDefaultAddress.host.get}".replace(".", "_"))
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS)
          .filter(MetricFilter.ALL)
          .build(graphite)

        LOG.info(s"Metrics is enabled..., reporting to $graphiteHost, $graphitePort")

        reporter.start(5, TimeUnit.SECONDS)

        system.registerOnTermination(new Runnable {
          override def run = reporter.stop()
        })
      }

      startReporter
    }

    meters
  }
}