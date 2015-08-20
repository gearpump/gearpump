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

package io.gearpump.metrics

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ExtendedActorSystem, Actor}
import io.gearpump.codahale.metrics.{Slf4jReporter, MetricFilter, ScheduledReporter}
import io.gearpump.codahale.metrics.graphite.{GraphiteReporter, Graphite}
import io.gearpump.metrics.Metrics.{ReportMetrics, DemandMoreMetrics}
import io.gearpump.metrics.MetricsReporterService.{ReportTo}
import io.gearpump.util.Constants._
import io.gearpump.util.LogUtil
import scala.concurrent.duration._

class MetricsReporterService(metrics: Metrics) extends Actor {

  private val LOG = LogUtil.getLogger(getClass)
  private implicit val system = context.system

  private val reportInterval = system.settings.config.getInt(GEARPUMP_METRIC_REPORT_INTERVAL)
  private val reporter = getReporter
  implicit val dispatcher = context.dispatcher

  def receive: Receive = {
    case DemandMoreMetrics(subscriber) => {
      reporter.report(subscriber)
      context.system.scheduler.scheduleOnce(reportInterval milliseconds,
        subscriber, ReportMetrics)
    }
  }

  def startGraphiteReporter = {
    val graphiteHost = system.settings.config.getString(GEARPUMP_METRIC_GRAPHITE_HOST)
    val graphitePort = system.settings.config.getInt(GEARPUMP_METRIC_GRAPHITE_PORT)

    val graphite = new Graphite(new InetSocketAddress(graphiteHost, graphitePort))
    LOG.info(s"reporting to $graphiteHost, $graphitePort")
    new ReportTo {
      private val reporter = GraphiteReporter.forRegistry(metrics.registry)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .filter(MetricFilter.ALL)
        .build(graphite)

      override def report(to: ActorRef): Unit = reporter.report()
    }
  }

  def startSlf4jReporter = {
    new ReportTo {
      val reporter = Slf4jReporter.forRegistry(metrics.registry)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .filter(MetricFilter.ALL)
        .outputTo(LOG)
        .build()

      override def report(to: ActorRef): Unit = reporter.report()
    }
  }

  def startAkkaReporter = {
    new AkkaReporter(system, metrics.registry)
  }

  def getReporter: ReportTo = {
    val reporterType = system.settings.config.getString(GEARPUMP_METRIC_REPORTER)
    LOG.info(s"Metrics reporter is enabled, using $reporterType reporter")
    val reporter = reporterType match {
      case "graphite" => startGraphiteReporter
      case "logfile" => startSlf4jReporter
      case "akka" => startAkkaReporter
    }
    reporter
  }
}

object MetricsReporterService {
  trait ReportTo{
    def report(to: ActorRef): Unit
  }
}