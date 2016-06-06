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

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

import akka.actor.{Actor, ActorRef}

import org.apache.gearpump.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import org.apache.gearpump.codahale.metrics.{MetricFilter, Slf4jReporter}
import org.apache.gearpump.metrics.Metrics.{DemandMoreMetrics, ReportMetrics}
import org.apache.gearpump.metrics.MetricsReporterService.ReportTo
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.LogUtil

/**
 * Reports the metrics data to some where, like Ganglia, remote Akka actor, log files...
 *
 * @param metrics Holds a list of metrics object.
 */
class MetricsReporterService(metrics: Metrics) extends Actor {

  private val LOG = LogUtil.getLogger(getClass)
  private implicit val system = context.system

  private val reportInterval = system.settings.config.getInt(GEARPUMP_METRIC_REPORT_INTERVAL)
  private val reporter = getReporter
  implicit val dispatcher = context.dispatcher

  def receive: Receive = {
    // The subscriber is demanding more messages.
    case DemandMoreMetrics(subscriber) => {
      reporter.report(subscriber)
      context.system.scheduler.scheduleOnce(reportInterval.milliseconds,
        subscriber, ReportMetrics)
    }
  }

  def startGraphiteReporter(): ReportTo = {
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

  def startSlf4jReporter(): ReportTo = {
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

  def startAkkaReporter(): ReportTo = {
    new AkkaReporter(system, metrics.registry)
  }

  def getReporter: ReportTo = {
    val reporterType = system.settings.config.getString(GEARPUMP_METRIC_REPORTER)
    LOG.info(s"Metrics reporter is enabled, using $reporterType reporter")
    val reporter = reporterType match {
      case "graphite" => startGraphiteReporter()
      case "logfile" => startSlf4jReporter()
      case "akka" => startAkkaReporter()
    }
    reporter
  }
}

object MetricsReporterService {

  /** Target where user want to report the metrics data to */
  trait ReportTo {
    def report(to: ActorRef): Unit
  }
}