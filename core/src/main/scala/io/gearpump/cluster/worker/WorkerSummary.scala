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

package io.gearpump.cluster.worker

import io.gearpump.util.HistoryMetricsService.HistoryMetricsConfig

/**
 * Worker summary information for REST API.
 */
case class WorkerSummary(
    workerId: WorkerId,
    state: String,
    actorPath: String,
    aliveFor: Long,
    logFile: String,
    executors: Array[ExecutorSlots],
    totalSlots: Int,
    availableSlots: Int,
    homeDirectory: String,
    jvmName: String,
    // Id used to uniquely identity this worker process in low level resource manager like YARN.
    resourceManagerContainerId: String,
    historyMetricsConfig: HistoryMetricsConfig = null)

object WorkerSummary {
  def empty: WorkerSummary = {
    WorkerSummary(WorkerId.unspecified, "", "", 0L, "",
      Array.empty[ExecutorSlots], 0, 0, "", jvmName = "", resourceManagerContainerId = "")
  }
}

case class ExecutorSlots(appId: Int, executorId: Int, slots: Int)