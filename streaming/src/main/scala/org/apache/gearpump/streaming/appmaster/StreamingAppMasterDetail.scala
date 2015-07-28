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

package org.apache.gearpump.streaming.appmaster

import org.apache.gearpump._
import org.apache.gearpump.cluster.AppMasterToMaster.AppMasterDataDetail
import org.apache.gearpump.cluster.MasterToAppMaster
import org.apache.gearpump.cluster.MasterToAppMaster.AppMasterStatus
import org.apache.gearpump.partitioner.{PartitionerByClassName, PartitionerDescription, Partitioner}
import org.apache.gearpump.streaming._
import org.apache.gearpump.streaming.task.TaskId
import org.apache.gearpump.util.Graph

case class StreamingAppMasterDataDetail(
    appId: Int,
    appName: String = null,
    processors: Map[ProcessorId, ProcessorDescription],
    // hiearachy level for each processor
    processorLevels: Map[ProcessorId, Int],
    dag: Graph[ProcessorId, String] = null,
    actorPath: String = null,
    clock: TimeStamp = 0,
    executors: Map[ExecutorId, String] = null,
    tasks: Map[TaskId, ExecutorId] = null,
    status: AppMasterStatus = MasterToAppMaster.AppMasterActive,
    startTime: TimeStamp = 0L,
    user: String = null)
  extends AppMasterDataDetail