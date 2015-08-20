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

package io.gearpump.util

import java.util

import akka.actor.Actor
import com.typesafe.config.Config
import io.gearpump.TimeStamp
import io.gearpump.cluster.ClientToMaster.QueryHistoryMetrics
import io.gearpump.cluster.MasterToClient.{HistoryMetrics, HistoryMetricsItem}
import io.gearpump.metrics.Metrics._
import io.gearpump.util.Constants._
import io.gearpump.util.HistoryMetricsService.{MetricsStore, HistoryMetricsConfig}
import org.slf4j.Logger

import scala.collection.mutable.ListBuffer

/**
 * Metrics service to serve history metrics data
 */
class HistoryMetricsService(name: String, config: HistoryMetricsConfig) extends Actor {
  private val LOG: Logger = LogUtil.getLogger(getClass, name = name)
  private var metricsStore = Map.empty[String, MetricsStore]

  def receive: Receive = metricHandler orElse commandHandler
  def metricHandler: Receive = {
    case ReportMetrics =>
      sender ! DemandMoreMetrics(self)
    case metrics: MetricType =>
      val name = metrics.name
      if (metricsStore.contains(name)) {
        metricsStore(name).add(metrics)
      } else {
        val store = MetricsStore(name, metrics, config)
        metricsStore += name -> store
        store.add(metrics)
      }
  }

  private def toRegularExpression(input: String): String = {
    "^" + input.flatMap {
      case '*' => ".*"
      case '?' => "."
      case char if "()[]$^.{}|\\".contains(char) => "\\" + char
      case other => s"$other"
    } + ".*$"
  }

  private def fetchMetricsHistory(pathPattern: String, readLastest: Boolean = false): List[HistoryMetricsItem] = {

    val result = new ListBuffer[HistoryMetricsItem]
    val regex = toRegularExpression(pathPattern)

    metricsStore.keys.foreach { name =>
      if (name.matches(regex)) {
        if (readLastest) {
          result.append(metricsStore(name).readLatest: _*)
        } else {
          result.append(metricsStore(name).read: _*)
        }
      }
    }
    result.toList
  }

  def commandHandler: Receive = {

    //path accept syntax ? *, ? will match one char, * will match at least one char
    case QueryHistoryMetrics(inputPath, readLatest) =>
      sender ! HistoryMetrics(inputPath, fetchMetricsHistory(inputPath, readLatest))
   }
}

object HistoryMetricsService {

  /**
   * For simplicity, HistoryMetricsService will maintain 72 hours coarse-grained data
   * for last 72 hours, and fine-grained data for past 5 min.
   *
   * For the coarse-grained data of past 72 hours, one or two sample point will be stored
   * for each hour.
   *
   * For Counter: we will store one data point per hour.
   * For Meter, we will store two data points per hour, with one point which have
   * max mean value, the other point with min mean value.
   * For Histogram: we will store two data points per hour, with one point which have
   * max mean value, the other point with min mean value.
   *
   * It is designed like this so that we are able to maintain abnormal metrics pattern,
   * Like a sudden rise in latency, so a sudden drop in throughput.
   *
   * For fine-grained data in last 5 min, there will be 1 sample point per 15 seconds.
   *
   */
  trait MetricsStore {
    def add(inputMetrics: MetricType): Unit

    def read: List[HistoryMetricsItem]

    /**
     * read latest inserted records
     * @return
     */
    def readLatest: List[HistoryMetricsItem]
  }

  object MetricsStore {
    def apply(name: String, metric: MetricType, config: HistoryMetricsConfig): MetricsStore = {
      metric match {
        case histogram: Histogram => new HistogramMetricsStore(config)
        case meter: Meter => new MeterMetricsStore(config)
        case counter: Counter => new CounterMetricsStore(config)
        case gauge: Gauge => new GaugeMetricsStore(config)
      }
    }
  }

  /**
   * min, and max data point for current time window (startTimeMs, startTimeMs + interval)
   *
   * @param startTimeMs
   * @param min
   * @param max
   */
  case class MinMaxMetrics(startTimeMs: Long, min: HistoryMetricsItem, max: HistoryMetricsItem)

  /**
   * Metrics store to store history data points
   * For each time point, we will store two data points, with one min, and one max.
   *
   * @param retainCount how many data points to retain, old data will be removed
   * @param retainIntervalMs time interval between two data points.
   * @param compare (left, right) => true, return true when left > right
   *   We should compare to decide which data point to keep in current time interval.
   *   The data point which is max or min in value will be kept.
   *
   */
  class MinMaxMetricsStore(
      retainCount: Int,
      retainIntervalMs: Long,
      compare: (HistoryMetricsItem, HistoryMetricsItem) => Boolean)
    extends MetricsStore{

    val queue = new util.ArrayDeque[MinMaxMetrics]()
    private var latest = List.empty[HistoryMetricsItem]

    override def add(inputMetrics: MetricType): Unit = add(inputMetrics, System.currentTimeMillis())

    def add(inputMetrics: MetricType, now: TimeStamp): Unit = {
      val metrics = HistoryMetricsItem(now, inputMetrics)
      latest = List(metrics)

      val head = queue.peek()
      if (head == null || now - head.startTimeMs > retainIntervalMs) {
        //insert new data point to head
        queue.addFirst(MinMaxMetrics(now / retainIntervalMs * retainIntervalMs, metrics, metrics))
        // remove old data if necessary
        if (queue.size() > retainCount) {
          queue.removeLast()
        }
      } else {
        updateHead(metrics)
      }
    }

    private def updateHead(metrics: HistoryMetricsItem) = {
      val head = queue.poll()
      if (compare(metrics, head.max)) {
        queue.addFirst(MinMaxMetrics(head.startTimeMs, head.min, metrics))
      } else if (compare(metrics, head.min)) {
        queue.addFirst(MinMaxMetrics(head.startTimeMs, metrics, head.max))
      }
    }

    def read: List[HistoryMetricsItem] = {
      val result = new ListBuffer[HistoryMetricsItem]
      import scala.collection.JavaConversions.asScalaIterator
      queue.iterator.foreach {pair =>
          result.prepend(pair.max)
        result.prepend(pair.min)
      }
      result.toList
    }

    override def readLatest: List[HistoryMetricsItem] = {
      latest
    }
  }

  /**
   ** Metrics store to store history data points
   * For each time point, we will store single data point.
   *
   * @param retainCount how many data points to retain, old data will be removed
   * @param retainIntervalMs time interval between two data points.
   */
  class SingleValueMetricsStore (retainCount: Int, retainIntervalMs: Long) extends MetricsStore{

    private val queue =  new util.ArrayDeque[HistoryMetricsItem]()
    private var latest = List.empty[HistoryMetricsItem]

    override def add(inputMetrics: MetricType): Unit = {
      add(inputMetrics, System.currentTimeMillis())
    }

    def add(inputMetrics: MetricType, now: TimeStamp): Unit = {
      val head = queue.peek()
      val metrics = HistoryMetricsItem(now, inputMetrics)
      latest = List(metrics)

      if (head == null || now - head.time > retainIntervalMs) {

        queue.addFirst(metrics)

        // remove old data
        if (queue.size() > retainCount) {
          queue.removeLast()
        }
      }

    }

    def read: List[HistoryMetricsItem] = {
      val result = new ListBuffer[HistoryMetricsItem]
      import scala.collection.JavaConversions.asScalaIterator
      queue.iterator().foreach(result.prepend(_))
      result.toList
    }

    override def readLatest: List[HistoryMetricsItem] = {
      latest
    }
  }

  /**
   *
   * @param retainHistoryDataHours Retain at max @RETAIN_HISTORY_HOURS history data(unit hour)
   * @param retainHistoryDataIntervalMs time interval between two history data points.(unit: ms)
   * @param retainRecentDataSeconds Retain at max @RETAIN_LATEST_SECONDS recent data points(unit: seconds)
   * @param retainRecentDataIntervalMs Retain at max @RETAIN_LATEST_SECONDS recent data points(unit: ms)
   */
  case class HistoryMetricsConfig(
      retainHistoryDataHours: Int,
      retainHistoryDataIntervalMs: Int,
      retainRecentDataSeconds: Int,
      retainRecentDataIntervalMs: Int)

  object HistoryMetricsConfig {
    def apply(config: Config): HistoryMetricsConfig = {
      val historyHour = config.getInt(GEARPUMP_METRIC_RETAIN_HISTORY_DATA_HOURS)
      val historyInterval = config.getInt(GEARPUMP_RETAIN_HISTORY_DATA_INTERVAL_MS)

      val recentSeconds = config.getInt(GEARPUMP_RETAIN_RECENT_DATA_SECONDS)
      val recentInterval = config.getInt(GEARPUMP_RETAIN_RECENT_DATA_INTERVAL_MS)
      HistoryMetricsConfig(historyHour, historyInterval, recentSeconds, recentInterval)
    }
  }

  class HistogramMetricsStore(config: HistoryMetricsConfig) extends MetricsStore {

    private val compartor = (left: HistoryMetricsItem, right: HistoryMetricsItem) =>
      left.value.asInstanceOf[Histogram].mean > right.value.asInstanceOf[Histogram].mean

    private val history = new MinMaxMetricsStore(
      config.retainHistoryDataHours * 3600 * 1000 / config.retainHistoryDataIntervalMs,
      config.retainHistoryDataIntervalMs, compartor)

    private val recent = new SingleValueMetricsStore(
      config.retainRecentDataSeconds * 1000 / config.retainRecentDataIntervalMs,
      config.retainRecentDataIntervalMs)

    override def add(inputMetrics: MetricType): Unit = {
      recent.add(inputMetrics)
      history.add(inputMetrics)
    }

    override def read: List[HistoryMetricsItem] = {
      history.read ++ recent.read
    }

    override def readLatest: List[HistoryMetricsItem] = {
      recent.readLatest
    }
  }

  class MeterMetricsStore(config: HistoryMetricsConfig) extends MetricsStore {

    private val compartor = (left: HistoryMetricsItem, right: HistoryMetricsItem) =>
      left.value.asInstanceOf[Meter].meanRate > right.value.asInstanceOf[Meter].meanRate

    private val history = new MinMaxMetricsStore(
      config.retainHistoryDataHours * 3600 * 1000 / config.retainHistoryDataIntervalMs,
      config.retainHistoryDataIntervalMs, compartor)

    private val recent = new SingleValueMetricsStore(
      config.retainRecentDataSeconds * 1000 / config.retainRecentDataIntervalMs,
      config.retainRecentDataIntervalMs)

    override def add(inputMetrics: MetricType): Unit = {
      recent.add(inputMetrics)
      history.add(inputMetrics)
    }

    override def read: List[HistoryMetricsItem] = {
      history.read ++ recent.read
    }

    override def readLatest: List[HistoryMetricsItem] = {
      recent.readLatest
    }
  }

  class CounterMetricsStore(config: HistoryMetricsConfig) extends MetricsStore {

    private val history = new SingleValueMetricsStore(
      config.retainHistoryDataHours * 3600 * 1000 / config.retainHistoryDataIntervalMs,
      config.retainHistoryDataIntervalMs)

    private val recent = new SingleValueMetricsStore(
      config.retainRecentDataSeconds * 1000 / config.retainRecentDataIntervalMs,
      config.retainRecentDataIntervalMs)

    override def add(inputMetrics: MetricType): Unit = {
      history.add(inputMetrics)
      recent.add(inputMetrics)
    }

    override def read: List[HistoryMetricsItem] = {
      history.read ++ recent.read
    }

    override def readLatest: List[HistoryMetricsItem] = {
      recent.readLatest
    }
  }

  class GaugeMetricsStore(config: HistoryMetricsConfig) extends MetricsStore {

    private val compartor = (left: HistoryMetricsItem, right: HistoryMetricsItem) =>
      left.value.asInstanceOf[Gauge].value > right.value.asInstanceOf[Gauge].value

    private val history = new MinMaxMetricsStore(
      config.retainHistoryDataHours * 3600 * 1000 / config.retainHistoryDataIntervalMs,
      config.retainHistoryDataIntervalMs, compartor)

    private val recent = new SingleValueMetricsStore(
      config.retainRecentDataSeconds * 1000 / config.retainRecentDataIntervalMs,
      config.retainRecentDataIntervalMs)

    override def add(inputMetrics: MetricType): Unit = {
      recent.add(inputMetrics)
      history.add(inputMetrics)
    }

    override def read: List[HistoryMetricsItem] = {
      history.read ++ recent.read
    }

    override def readLatest: List[HistoryMetricsItem] = {
      recent.readLatest
    }
  }
}
