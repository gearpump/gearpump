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
package io.gearpump.cluster.scheduler

import akka.actor.{Actor, ActorRef}
import io.gearpump.Time.MilliSeconds
import io.gearpump.cluster.MasterToWorker.{UpdateResourceFailed, UpdateResourceSucceed, WorkerRegistered}
import io.gearpump.cluster.WorkerToMaster.ResourceUpdate
import io.gearpump.cluster.master.Master.WorkerTerminated
import io.gearpump.cluster.scheduler.Scheduler.ApplicationFinished
import io.gearpump.cluster.worker.WorkerId
import io.gearpump.util.LogUtil
import org.slf4j.Logger
import scala.collection.mutable

/**
 * Scheduler schedule resource for different applications.
 */
abstract class Scheduler extends Actor {
  val LOG: Logger = LogUtil.getLogger(getClass)
  protected var resources = new mutable.HashMap[WorkerId, (ActorRef, Resource)]

  def handleScheduleMessage: Receive = {
    case WorkerRegistered(id, _) =>
      if (!resources.contains(id)) {
        LOG.info(s"Worker $id added to the scheduler")
        resources.put(id, (sender, Resource.empty))
      }
    case update@ResourceUpdate(worker, workerId, resource) =>
      LOG.info(s"$update...")
      if (resources.contains(workerId)) {
        val resourceReturned = resource > resources.get(workerId).get._2
        resources.update(workerId, (worker, resource))
        if (resourceReturned) {
          allocateResource()
        }
        sender ! UpdateResourceSucceed
      }
      else {
        sender ! UpdateResourceFailed(
          s"ResourceUpdate failed! The worker $workerId has not been registered into master")
      }
    case WorkerTerminated(workerId) =>
      if (resources.contains(workerId)) {
        resources -= workerId
      }
    case ApplicationFinished(appId) =>
      doneApplication(appId)
  }

  def allocateResource(): Unit

  def doneApplication(appId: Int): Unit
}

object Scheduler {
  case class PendingRequest(
      appId: Int, appMaster: ActorRef, request: ResourceRequest, timeStamp: MilliSeconds)

  case class ApplicationFinished(appId: Int)
}