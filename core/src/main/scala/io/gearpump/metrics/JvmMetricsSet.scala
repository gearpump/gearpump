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

package io.gearpump.metrics

import com.codahale.metrics.{Metric, MetricSet}
import com.codahale.metrics.jvm.{MemoryUsageGaugeSet, ThreadStatesGaugeSet}
import java.util
import scala.collection.JavaConverters._

class JvmMetricsSet(name: String) extends MetricSet {

  override def getMetrics: util.Map[String, Metric] = {
    val memoryMetrics = new MemoryUsageGaugeSet().getMetrics.asScala
    val threadMetrics = new ThreadStatesGaugeSet().getMetrics.asScala
    Map(
      s"$name:memory.total.used" -> memoryMetrics("total.used"),
      s"$name:memory.total.committed" -> memoryMetrics("total.committed"),
      s"$name:memory.total.max" -> memoryMetrics("total.max"),
      s"$name:memory.heap.used" -> memoryMetrics("heap.used"),
      s"$name:memory.heap.committed" -> memoryMetrics("heap.committed"),
      s"$name:memory.heap.max" -> memoryMetrics("heap.max"),
      s"$name:thread.count" -> threadMetrics("count"),
      s"$name:thread.daemon.count" -> threadMetrics("daemon.count")
    ).asJava
  }
}
