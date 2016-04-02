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
package io.gearpump.cluster

import akka.actor.ActorRef
import io.gearpump.WorkerId
import io.gearpump.cluster.master.Master.MasterInfo
import io.gearpump.cluster.scheduler.Resource

/**
 * Cluster Bootup Flow
 */
object WorkerToMaster {
  case object RegisterNewWorker
  case class RegisterWorker(workerId: WorkerId)
  case class ResourceUpdate(worker: ActorRef, workerId: WorkerId, resource: Resource)
}

object MasterToWorker {
  case class WorkerRegistered(workerId : WorkerId, masterInfo: MasterInfo)
  case class UpdateResourceFailed(reason : String = null, ex: Throwable = null)
  case object UpdateResourceSucceed
}