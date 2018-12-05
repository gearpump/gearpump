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
package io.gearpump.cluster

import akka.actor.ActorRef
import io.gearpump.cluster.master.Master.MasterInfo
import io.gearpump.cluster.scheduler.Resource
import io.gearpump.cluster.worker.WorkerId

/**
 * Cluster Bootup Flow
 */
object WorkerToMaster {

  /** When an worker is started, it sends RegisterNewWorker */
  case object RegisterNewWorker

  /** When worker lose connection with master, it tries to register itself again with old Id. */
  case class RegisterWorker(workerId: WorkerId)

  /** Worker is responsible to broadcast its current status to master */
  case class ResourceUpdate(worker: ActorRef, workerId: WorkerId, resource: Resource)
}

object MasterToWorker {

  /** Master confirm the reception of RegisterNewWorker or RegisterWorker */
  case class WorkerRegistered(workerId: WorkerId, masterInfo: MasterInfo)

  /** Worker have not received reply from master */
  case class UpdateResourceFailed(reason: String = null, ex: Throwable = null)

  /** Master is synced with worker on resource slots managed by current worker */
  case object UpdateResourceSucceed
}