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

package org.apache.gearpump.streaming.metrics

import java.util

import com.typesafe.config.Config

import org.apache.gearpump.TimeStamp
import org.apache.gearpump.cluster.ClientToMaster.ReadOption
import org.apache.gearpump.cluster.MasterToClient.HistoryMetricsItem
import org.apache.gearpump.google.common.collect.Iterators
import org.apache.gearpump.metrics.Metrics.{Histogram, Meter}
import org.apache.gearpump.metrics.MetricsAggregator
import org.apache.gearpump.streaming.metrics.ProcessorAggregator._
import org.apache.gearpump.util.HistoryMetricsService.HistoryMetricsConfig

/**
 *
 * Does aggregation on metrics after grouping by these three attributes:
 *  1. processorId
 *  2. time section(represented as a index integer)
 *  3. metricName(like sendThroughput)
 *
 * It assumes that for each [[org.apache.gearpump.cluster.MasterToClient.HistoryMetricsItem]], the
 * name follow the format app(appId).processor(processorId).task(taskId).(metricName)
 *
 * It parses the name to get processorId and metricName. If the parsing fails, then current
 * [[org.apache.gearpump.cluster.MasterToClient.HistoryMetricsItem]] will be skipped.
 *
 * NOTE: this class is optimized for performance.
 */
class ProcessorAggregator(historyMetricConfig: HistoryMetricsConfig) extends MetricsAggregator {

  def this(config: Config) = {
    this(HistoryMetricsConfig(config))
  }

  private val aggregatorFactory: AggregatorFactory = new AggregatorFactory()

  /**
   * Accepts options:
   * key: "readOption", value: one of "readLatest", "readRecent", "readHistory"
   */
  override def aggregate(options: Map[String, String],
      inputs: Iterator[HistoryMetricsItem]): List[HistoryMetricsItem] = {
    val readOption = options.get(ReadOption.Key).getOrElse(ReadOption.ReadLatest)
    aggregate(readOption, inputs, System.currentTimeMillis())
  }

  def aggregate(
      readOption: ReadOption.ReadOption, inputs: Iterator[HistoryMetricsItem], now: TimeStamp)
    : List[HistoryMetricsItem] = {
    val (start, end, interval) = getTimeRange(readOption, now)
    val timeSlotsCount = ((end - start - 1) / interval + 1).toInt
    val map = new MultiLayerMap[Aggregator](timeSlotsCount)

    val taskIdentity = new TaskIdentity(0, null)
    while (inputs.hasNext) {
      val item = inputs.next()

      if (item.value.isInstanceOf[Meter] || item.value.isInstanceOf[Histogram]) {
        if (item.time >= start && item.time < end) {
          val timeIndex = ((item.time - start) / interval).toInt

          if (parseName(item.value.name, taskIdentity)) {
            var op = map.get(timeIndex, taskIdentity.group)
            if (op == null) {
              op = aggregatorFactory.create(item, taskIdentity.group)
              map.put(timeIndex, taskIdentity.group, op)
            }
            op.aggregate(item)
          }
        }
      }
    }

    val result = new Array[HistoryMetricsItem](map.size)
    val iterator = map.valueIterator
    var index = 0
    while (iterator.hasNext()) {
      val op = iterator.next()
      result(index) = op.result
      index += 1
    }

    result.toList
  }

  // Returns (start, end, interval)
  private def getTimeRange(readOption: ReadOption.ReadOption, now: TimeStamp)
    : (TimeStamp, TimeStamp, TimeStamp) = {
    readOption match {
      case ReadOption.ReadRecent =>
        val end = now
        val start = end - (historyMetricConfig.retainRecentDataSeconds) * 1000
        val interval = historyMetricConfig.retainRecentDataIntervalMs
        (floor(start, interval), floor(end, interval), interval)
      case ReadOption.ReadHistory =>
        val end = now
        val start = end - (historyMetricConfig.retainHistoryDataHours) * 3600 * 1000
        val interval = historyMetricConfig.retainHistoryDataIntervalMs
        (floor(start, interval), floor(end, interval), interval)
      case _ =>
        // All data points are aggregated together.
        (0L, Long.MaxValue, Long.MaxValue)
    }
  }

  // The original metrics data is divided by interval points:
  // time series (0, interval, 2*interval, 3*interval....)
  // floor(..) make sure the Aggregator use the same set of interval points.
  private def floor(value: Long, interval: Long): Long = {
    (value / interval) * interval
  }

  // Returns "app0.processor0:sendThroughput" as the group Id.
  private def parseName(name: String, result: TaskIdentity): Boolean = {
    val taskIndex = name.indexOf(TASK_TAG)
    if (taskIndex > 0) {
      val processor = name.substring(0, taskIndex)
      val typeIndex = name.indexOf(":", taskIndex + 1)
      if (typeIndex > 0) {
        result.task = (name.substring(taskIndex + TASK_TAG.length, typeIndex)).toShort
        val metricName = name.substring(typeIndex)
        result.group = processor + metricName
        true
      } else {
        false
      }
    } else {
      false
    }
  }
}

object ProcessorAggregator {
  val readOption = ReadOption.Key

  private val TASK_TAG = ".task"

  private class TaskIdentity(var task: Short, var group: String)

  /**
   *
   * MultiLayerMap has multiple layers. For each layer, there
   * is a hashMap.
   *
   * To access a value with get, user need to specify first layer Id, then key.
   *
   * This class is optimized for performance.
   */
  class MultiLayerMap[Value](layers: Int) {

    private var _size: Int = 0
    private val map: Array[java.util.HashMap[String, Value]] = createMap(layers)

    /**
     * @param key key in current layer
     * @return return null if key is not found
     */
    def get(layer: Int, key: String): Value = {
      if (layer < layers) {
        map(layer).get(key)
      } else {
        null.asInstanceOf[Value]
      }
    }

    def put(layer: Int, key: String, value: Value): Unit = {
      if (layer < layers) {
        map(layer).put(key, value)
        _size += 1
      }
    }

    def size: Int = _size

    def valueIterator: util.Iterator[Value] = {
      val iterators = new Array[util.Iterator[Value]](layers)
      var layer = 0
      while (layer < layers) {
        iterators(layer) = map(layer).values().iterator()
        layer += 1
      }

      Iterators.concat(iterators: _*)
    }

    private def createMap(layers: Int) = {
      val map = new Array[java.util.HashMap[String, Value]](layers)
      var index = 0
      val length = map.length
      while (index < length) {
        map(index) = new java.util.HashMap[String, Value]()
        index += 1
      }
      map
    }
  }

  trait Aggregator {
    def aggregate(item: HistoryMetricsItem): Unit
    def result: HistoryMetricsItem
  }

  class HistogramAggregator(name: String) extends Aggregator {

    var count: Long = 0
    var mean: Double = 0
    var stddev: Double = 0
    var median: Double = 0
    var p95: Double = 0
    var p99: Double = 0
    var p999: Double = 0

    var startTime: TimeStamp = Long.MaxValue

    override def aggregate(item: HistoryMetricsItem): Unit = {
      val input = item.value.asInstanceOf[Histogram]
      count += 1
      mean += input.mean
      stddev += input.stddev
      median += input.median
      p95 += input.p95
      p99 += input.p99
      p999 += input.p999

      if (item.time < startTime) {
        startTime = item.time
      }
    }

    override def result: HistoryMetricsItem = {
      if (count > 0) {
        HistoryMetricsItem(startTime, Histogram(name, mean / count, stddev / count,
          median / count, p95 / count, p99 / count, p999 / count))
      } else {
        HistoryMetricsItem(0, Histogram(name, 0, 0, 0, 0, 0, 0))
      }
    }
  }

  class MeterAggregator(name: String) extends Aggregator {

    var count: Long = 0
    var meanRate: Double = 0
    var m1: Double = 0
    var rateUnit: String = null

    var startTime: TimeStamp = Long.MaxValue

    override def aggregate(item: HistoryMetricsItem): Unit = {

      val input = item.value.asInstanceOf[Meter]
      count += input.count

      meanRate += input.meanRate
      m1 += input.m1

      if (null == rateUnit) {
        rateUnit = input.rateUnit
      }

      if (item.time < startTime) {
        startTime = item.time
      }
    }

    override def result: HistoryMetricsItem = {
      HistoryMetricsItem(startTime, Meter(name, count, meanRate,
        m1, rateUnit))
    }
  }

  class AggregatorFactory {
    def create(item: HistoryMetricsItem, name: String): Aggregator = {
      item.value match {
        case meter: Meter => new MeterAggregator(name)
        case histogram: Histogram => new HistogramAggregator(name)
        case _ => null // not supported
      }
    }
  }
}