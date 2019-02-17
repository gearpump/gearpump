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

package io.gearpump.util

import akka.actor.Actor
import com.typesafe.config.Config
import io.gearpump.Time.MilliSeconds
import io.gearpump.cluster.ClientToMaster.{QueryHistoryMetrics, ReadOption}
import io.gearpump.cluster.MasterToClient.{HistoryMetrics, HistoryMetricsItem}
import io.gearpump.metrics.Metrics._
import io.gearpump.metrics.MetricsAggregator
import io.gearpump.util.Constants._
import io.gearpump.util.HistoryMetricsService.{DummyMetricsAggregator, HistoryMetricsConfig, HistoryMetricsStore, SkipAllAggregator}
import java.util
import org.slf4j.Logger
import scala.collection.mutable.ListBuffer

/**
 *
 * Metrics service to serve history metrics data
 *
 * For simplicity, HistoryMetricsService will maintain 72 hours coarse-grained data
 * for last 72 hours, and fine-grained data for past 5 min.
 *
 * For the coarse-grained data of past 72 hours, one or two sample point will be stored
 * for each hour.
 *
 * For fine-grained data in last 5 min, there will be 1 sample point per 15 seconds.
 */
class HistoryMetricsService(name: String, config: HistoryMetricsConfig) extends Actor {
  private val LOG: Logger = LogUtil.getLogger(getClass, name = name)
  private var metricsStore = Map.empty[String, HistoryMetricsStore]
  private val systemConfig = context.system.settings.config

  def receive: Receive = metricHandler orElse commandHandler
  def metricHandler: Receive = {
    case ReportMetrics =>
      sender ! DemandMoreMetrics(self)
    case metrics: MetricType =>
      val name = metrics.name
      if (metricsStore.contains(name)) {
        metricsStore(name).add(metrics)
      } else {
        val store = HistoryMetricsStore(metrics, config)
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

  private def fetchMetricsHistory(pathPattern: String, readOption: ReadOption.ReadOption)
  : List[HistoryMetricsItem] = {

    val result = new ListBuffer[HistoryMetricsItem]

    val regex = toRegularExpression(pathPattern).r.pattern

    val iter = metricsStore.iterator
    while (iter.hasNext) {
      val (name, store) = iter.next()

      val matcher = regex.matcher(name)
      if (matcher.matches()) {
        readOption match {
          case ReadOption.ReadLatest =>
            result.append(store.readLatest: _*)
          case ReadOption.ReadRecent =>
            result.append(store.readRecent: _*)
          case ReadOption.ReadHistory =>
            result.append(store.readHistory: _*)
          case _ =>
          // Skip all other options.
        }
      }
    }
    result.toList
  }

  val dummyAggregator = new DummyMetricsAggregator
  private var aggregators: Map[String, MetricsAggregator] = Map.empty[String, MetricsAggregator]

  import scala.collection.JavaConverters._
  private val validAggregators: Set[String] = {
    val rootConfig = systemConfig.getConfig(Constants.GEARPUMP_METRICS_AGGREGATORS).root.unwrapped
    rootConfig.keySet().asScala.toSet
  }

  def commandHandler: Receive = {
    // Path accept syntax ? *, ? will match one char, * will match at least one char
    case QueryHistoryMetrics(inputPath, readOption, aggregatorClazz, options) =>

      val aggregator = {
        if (aggregatorClazz == null || aggregatorClazz.isEmpty) {
          dummyAggregator
        } else if (aggregators.contains(aggregatorClazz)) {
          aggregators(aggregatorClazz)
        } else if (validAggregators.contains(aggregatorClazz)) {
          val clazz = Class.forName(aggregatorClazz)
          val constructor = clazz.getConstructor(classOf[Config])
          val aggregator = constructor.newInstance(systemConfig).asInstanceOf[MetricsAggregator]
          aggregators += aggregatorClazz -> aggregator
          aggregator
        } else {
          LOG.error(s"Aggregator $aggregatorClazz is not in the white list ${validAggregators}, " +
            s"we will drop all messages. Please see config at ${GEARPUMP_METRICS_AGGREGATORS}")
          val skipAll = new SkipAllAggregator
          aggregators += aggregatorClazz -> new SkipAllAggregator
          skipAll
        }
      }

      val metrics = fetchMetricsHistory(inputPath, readOption).iterator
      sender ! HistoryMetrics(inputPath, aggregator.aggregate(options, metrics))
  }
}

object HistoryMetricsService {

  trait MetricsStore {
    def add(inputMetrics: MetricType): Unit

    def read: List[HistoryMetricsItem]

    /**
     * read latest inserted records
     * @return
     */
    def readLatest: List[HistoryMetricsItem]
  }

  trait HistoryMetricsStore {
    def add(inputMetrics: MetricType): Unit

    /**
     * read latest inserted records
     * @return
     */
    def readLatest: List[HistoryMetricsItem]

    def readRecent: List[HistoryMetricsItem]

    def readHistory: List[HistoryMetricsItem]
  }

  class DummyHistoryMetricsStore extends HistoryMetricsStore {

    val empty = List.empty[HistoryMetricsItem]

    override def add(inputMetrics: MetricType): Unit = Unit

    override def readRecent: List[HistoryMetricsItem] = empty

    /**
     * read latest inserted records
     * @return
     */
    override def readLatest: List[HistoryMetricsItem] = empty

    override def readHistory: List[HistoryMetricsItem] = empty
  }

  object HistoryMetricsStore {
    def apply(metric: MetricType, config: HistoryMetricsConfig)
      : HistoryMetricsStore = {
      metric match {
        case _: Histogram => new HistogramMetricsStore(config)
        case _: Meter => new MeterMetricsStore(config)
        case _: Counter => new CounterMetricsStore(config)
        case _: Gauge => new GaugeMetricsStore(config)
        case _ => new DummyHistoryMetricsStore // other metrics are not supported
      }
    }
  }

  /**
   * Metrics store to store history data points
   * For each time point, we will store single data point.
   *
   * @param retainCount how many data points to retain, old data will be removed
   * @param retainIntervalMs time interval between two data points.
   */
  class SingleValueMetricsStore(retainCount: Int, retainIntervalMs: Long) extends MetricsStore {

    private val queue = new util.ArrayDeque[HistoryMetricsItem]()
    private var latest = List.empty[HistoryMetricsItem]

    // End of the time window we are tracking
    private var endTime = 0L

    override def add(inputMetrics: MetricType): Unit = {
      add(inputMetrics, System.currentTimeMillis())
    }

    def add(inputMetrics: MetricType, now: MilliSeconds): Unit = {

      val metrics = HistoryMetricsItem(now, inputMetrics)
      latest = List(metrics)

      if (now >= endTime) {
        queue.addFirst(metrics)
        endTime = (now / retainIntervalMs + 1) * retainIntervalMs

        // Removes old data
        if (queue.size() > retainCount) {
          queue.removeLast()
        }
      }
    }

    def read: List[HistoryMetricsItem] = {
      val result = new ListBuffer[HistoryMetricsItem]
      import scala.collection.JavaConverters._
      queue.iterator().asScala.foreach(result.prepend(_))
      result.toList
    }

    override def readLatest: List[HistoryMetricsItem] = {
      latest
    }
  }

  /**
   * Config for how long to keep history metrics data.
   *
   * @param retainHistoryDataHours Retain at max @RETAIN_HISTORY_HOURS history data(unit hour)
   * @param retainHistoryDataIntervalMs time interval between two history data points.(unit: ms)
   * @param retainRecentDataSeconds Retain at max @RETAIN_LATEST_SECONDS
   *                                recent data points(unit: seconds)
   * @param retainRecentDataIntervalMs Retain at max @RETAIN_LATEST_SECONDS recent
   *                                   data points(unit: ms)
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

  class HistogramMetricsStore(config: HistoryMetricsConfig) extends HistoryMetricsStore {

    private val history = new SingleValueMetricsStore(
      config.retainHistoryDataHours * 3600 * 1000 / config.retainHistoryDataIntervalMs,
      config.retainHistoryDataIntervalMs)

    private val recent = new SingleValueMetricsStore(
      config.retainRecentDataSeconds * 1000 / config.retainRecentDataIntervalMs,
      config.retainRecentDataIntervalMs)

    override def add(inputMetrics: MetricType): Unit = {
      recent.add(inputMetrics)
      history.add(inputMetrics)
    }

    override def readRecent: List[HistoryMetricsItem] = {
      recent.read
    }

    override def readHistory: List[HistoryMetricsItem] = {
      history.read
    }

    override def readLatest: List[HistoryMetricsItem] = {
      recent.readLatest
    }
  }

  class MeterMetricsStore(config: HistoryMetricsConfig) extends HistoryMetricsStore {

    private val history = new SingleValueMetricsStore(
      config.retainHistoryDataHours * 3600 * 1000 / config.retainHistoryDataIntervalMs,
      config.retainHistoryDataIntervalMs)

    private val recent = new SingleValueMetricsStore(
      config.retainRecentDataSeconds * 1000 / config.retainRecentDataIntervalMs,
      config.retainRecentDataIntervalMs)

    override def add(inputMetrics: MetricType): Unit = {
      recent.add(inputMetrics)
      history.add(inputMetrics)
    }

    override def readRecent: List[HistoryMetricsItem] = {
      recent.read
    }

    override def readHistory: List[HistoryMetricsItem] = {
      history.read
    }

    override def readLatest: List[HistoryMetricsItem] = {
      recent.readLatest
    }
  }

  class CounterMetricsStore(config: HistoryMetricsConfig) extends HistoryMetricsStore {

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

    override def readRecent: List[HistoryMetricsItem] = {
      recent.read
    }

    override def readHistory: List[HistoryMetricsItem] = {
      history.read
    }

    override def readLatest: List[HistoryMetricsItem] = {
      recent.readLatest
    }
  }

  class GaugeMetricsStore(config: HistoryMetricsConfig) extends HistoryMetricsStore {

    private val history = new SingleValueMetricsStore(
      config.retainHistoryDataHours * 3600 * 1000 / config.retainHistoryDataIntervalMs,
      config.retainHistoryDataIntervalMs)

    private val recent = new SingleValueMetricsStore(
      config.retainRecentDataSeconds * 1000 / config.retainRecentDataIntervalMs,
      config.retainRecentDataIntervalMs)

    override def add(inputMetrics: MetricType): Unit = {
      recent.add(inputMetrics)
      history.add(inputMetrics)
    }

    override def readRecent: List[HistoryMetricsItem] = {
      recent.read
    }

    override def readHistory: List[HistoryMetricsItem] = {
      history.read
    }

    override def readLatest: List[HistoryMetricsItem] = {
      recent.readLatest
    }
  }

  class DummyMetricsAggregator extends MetricsAggregator {
    def aggregate(options: Map[String, String], inputs: Iterator[HistoryMetricsItem])
      : List[HistoryMetricsItem] = {
      inputs.toList
    }
  }

  class SkipAllAggregator extends MetricsAggregator {
    private val empty = List.empty[HistoryMetricsItem]
    def aggregate(options: Map[String, String], inputs: Iterator[HistoryMetricsItem])
    : List[HistoryMetricsItem] = {
      empty
    }
  }
}
