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

package org.apache.gearpump.streaming.appmaster

import org.apache.gearpump._
import org.apache.gearpump.cluster.AppMasterToMaster.AppMasterSummary
import org.apache.gearpump.cluster.{ApplicationStatus, UserConfig}
import org.apache.gearpump.streaming.appmaster.AppMaster.ExecutorBrief
import org.apache.gearpump.streaming.{ExecutorId, LifeTime, ProcessorId}
import org.apache.gearpump.util.Graph
import org.apache.gearpump.util.HistoryMetricsService.HistoryMetricsConfig

/** Stream application summary, used for REST API */
case class StreamAppMasterSummary(
    appType: String = "streaming",
    appId: Int,
    appName: String = null,
    actorPath: String = null,
    clock: TimeStamp = 0L,
    status: ApplicationStatus = ApplicationStatus.ACTIVE,
    startTime: TimeStamp = 0L,
    uptime: TimeStamp = 0L,
    user: String = null,
    homeDirectory: String = "",
    logFile: String = "",
    dag: Graph[ProcessorId, String] = null,
    executors: List[ExecutorBrief] = null,
    processors: Map[ProcessorId, ProcessorSummary] = Map.empty[ProcessorId, ProcessorSummary],
    // Hiearachy level for each processor
    processorLevels: Map[ProcessorId, Int] = Map.empty[ProcessorId, Int],
    historyMetricsConfig: HistoryMetricsConfig = null)
  extends AppMasterSummary

case class TaskCount(count: Int)

case class ProcessorSummary(
    id: ProcessorId,
    taskClass: String,
    parallelism: Int,
    description: String,
    taskConf: UserConfig,
    life: LifeTime,
    executors: List[ExecutorId],
    taskCount: Map[ExecutorId, TaskCount])