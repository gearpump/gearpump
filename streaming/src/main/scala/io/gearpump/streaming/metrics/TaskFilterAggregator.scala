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

package io.gearpump.streaming.metrics

import com.typesafe.config.Config
import io.gearpump.cluster.ClientToMaster.ReadOption
import io.gearpump.cluster.MasterToClient.HistoryMetricsItem
import io.gearpump.metrics.MetricsAggregator
import io.gearpump.util.{Constants, LogUtil}
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
 * Filters the latest metrics data by specifying a
 * processor Id range, and taskId range.
 */
class TaskFilterAggregator(maxLimit: Int) extends MetricsAggregator {

  import TaskFilterAggregator._

  def this(config: Config) = {
    this(config.getInt(Constants.GEARPUMP_METRICS_MAX_LIMIT))
  }
  override def aggregate(options: Map[String, String], inputs: Iterator[HistoryMetricsItem])
    : List[HistoryMetricsItem] = {

    if (options.get(ReadOption.Key) != Some(ReadOption.ReadLatest)) {
      // Returns empty set
      List.empty[HistoryMetricsItem]
    } else {
      val parsed = Options.parse(options)
      if (parsed != null) {
        aggregate(parsed, inputs)
      } else {
        List.empty[HistoryMetricsItem]
      }
    }
  }

  def aggregate(options: Options, inputs: Iterator[HistoryMetricsItem])
    : List[HistoryMetricsItem] = {

    val result = new ListBuffer[HistoryMetricsItem]
    val effectiveLimit = Math.min(options.limit, maxLimit)
    var count = 0

    val taskIdentity = new TaskIdentity(0, 0)

    while (inputs.hasNext && count < effectiveLimit) {
      val item = inputs.next()
      if (parseName(item.value.name, taskIdentity)) {
        if (taskIdentity.processor >= options.startProcessor &&
          taskIdentity.processor < options.endProcessor &&
          taskIdentity.task >= options.startTask &&
          taskIdentity.task < options.endTask) {
          result.prepend(item)
          count += 1
        }
      }
    }
    result.toList
  }

  // Assume the name format is: "app0.processor0.task0:sendThroughput", returns
  // (processorId, taskId)
  //
  // returns true if success
  private def parseName(name: String, result: TaskIdentity): Boolean = {
    val processorStart = name.indexOf(PROCESSOR_TAG)
    if (processorStart != -1) {
      val taskStart = name.indexOf(TASK_TAG, processorStart + 1)
      if (taskStart != -1) {
        val processorId = name.substring(processorStart, taskStart).substring(PROCESSOR_TAG.length)
          .toInt
        result.processor = processorId
        val taskEnd = name.indexOf(":", taskStart + 1)
        if (taskEnd != -1) {
          val taskId = name.substring(taskStart, taskEnd).substring(TASK_TAG.length).toInt
          result.task = taskId
          true
        } else {
          false
        }
      } else {
        false
      }
    } else {
      false
    }
  }
}

object TaskFilterAggregator {
  val StartTask = "startTask"
  val EndTask = "endTask"
  val StartProcessor = "startProcessor"
  val EndProcessor = "endProcessor"
  val Limit = "limit"

  val TASK_TAG = ".task"
  val PROCESSOR_TAG = ".processor"

  private class TaskIdentity(var processor: Int, var task: Int)

  case class Options(
      limit: Int, startTask: Int, endTask: Int, startProcessor: Int, endProcessor: Int)

  private val LOG = LogUtil.getLogger(getClass)

  object Options {

    def acceptAll: Options = {
      new Options(Int.MaxValue, 0, Int.MaxValue, 0, Int.MaxValue)
    }

    def parse(options: Map[String, String]): Options = {
      // Do sanity check
      val optionTry = Try {
        val startTask = options.get(StartTask).map(_.toInt).getOrElse(0)
        val endTask = options.get(EndTask).map(_.toInt).getOrElse(Integer.MAX_VALUE)
        val startProcessor = options.get(StartProcessor).map(_.toInt).getOrElse(0)
        val endProcessor = options.get(EndProcessor).map(_.toInt).getOrElse(Integer.MAX_VALUE)
        val limit = options.get(Limit).map(_.toInt).getOrElse(DEFAULT_LIMIT)
        new Options(limit, startTask, endTask, startProcessor, endProcessor)
      }

      optionTry match {
        case Success(options) => options
        case Failure(ex) =>
          LOG.error("Failed to parse the options in TaskFilterAggregator. Error msg: " +
            ex.getMessage)
          null
      }
    }
  }

  val DEFAULT_LIMIT = 1000
  val MAX_LIMIT = 1000
}