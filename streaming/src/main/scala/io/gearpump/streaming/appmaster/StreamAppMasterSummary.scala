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

package io.gearpump.streaming.appmaster

import io.gearpump.streaming.{ExecutorId, ProcessorId, LifeTime}
import io.gearpump._
import io.gearpump.cluster.AppMasterToMaster.AppMasterSummary
import io.gearpump.cluster.{UserConfig, MasterToAppMaster}
import io.gearpump.cluster.MasterToAppMaster.AppMasterStatus
import AppMaster.ExecutorBrief
import io.gearpump.util.Graph
import io.gearpump.util.HistoryMetricsService.HistoryMetricsConfig

case class StreamAppMasterSummary(
    appId: Int,
    appName: String = null,
    processors: Map[ProcessorId, ProcessorSummary],
    // hiearachy level for each processor
    processorLevels: Map[ProcessorId, Int],
    dag: Graph[ProcessorId, String] = null,
    actorPath: String = null,
    clock: TimeStamp = 0,
    executors: List[ExecutorBrief] = null,
    status: AppMasterStatus = MasterToAppMaster.AppMasterActive,
    startTime: TimeStamp = 0L,
    uptime: TimeStamp = 0L,
    user: String = null,
    appType: String = "streaming",
    homeDirectory: String = "",
    logFile: String = "",
    historyMetricsConfig: HistoryMetricsConfig = null)
  extends AppMasterSummary

case class TaskCount(count: Int)

case class ProcessorSummary(
    id: ProcessorId,
    taskClass: String,
    parallelism : Int,
    description: String,
    taskConf: UserConfig,
    life: LifeTime,
    executors: List[ExecutorId],
    taskCount: Map[ExecutorId, TaskCount])