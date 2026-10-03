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

package io.gearpump.cluster.master

import org.apache.pekko.actor.{Actor, ActorRef, Status}
import org.apache.pekko.pattern.{ask, pipe}
import org.apache.pekko.util.Timeout
import io.gearpump.cluster.MasterToAppMaster.ResourceAllocated
import io.gearpump.security.{AllocateResource, ControlCapability, InstallLaunchGrant, LaunchGrantInstalled}
import scala.concurrent.Future
import scala.concurrent.duration._

/** Install worker grants before exposing an allocation to its authorized application. */
private[master] class AllocationBroker(allocation: AllocateResource, scheduler: ActorRef,
    requestor: ActorRef) extends Actor {
  import context.dispatcher
  implicit val timeout: Timeout = Timeout(15.seconds)
  private val secret = ControlCapability.token(context.system.settings.config, ControlCapability.AdminKey)
  private var remaining = allocation.request.request.resource.slots
  private var inFlight = 0
  private val deadline = context.system.scheduler.scheduleOnce(
    context.system.settings.config.getInt("gearpump.resource-allocation-timeout-seconds").seconds,
    self, Status.Failure(new java.util.concurrent.TimeoutException("Allocation timed out")))(context.dispatcher, self)
  scheduler ! allocation.request
  override def postStop(): Unit = deadline.cancel()
  override def receive: Receive = {
    case ResourceAllocated(resources) if sender() == scheduler =>
      inFlight += 1
      remaining -= resources.map(_.resource.slots).sum
      val installed = Future.sequence(resources.toSeq.map { resource =>
        val token = ControlCapability.random()
        val install = InstallLaunchGrant(secret, token, allocation.request.appId,
          resource.resource.slots, allocation.applicationCapability)
        (resource.worker ? install).map {
          case LaunchGrantInstalled(`token`) => resource.copy(allocationCapability = token)
          case _ => throw new SecurityException("Worker allocation denied")
        }
      })
      installed.map(resources => ResourceAllocated(resources.toArray)).pipeTo(self)(self)
    case result: ResourceAllocated if sender() == self =>
      requestor ! result
      inFlight -= 1
      if (remaining <= 0 && inFlight == 0) context.stop(self)
    case failure: Status.Failure if sender() == self => requestor ! failure; context.stop(self)
  }
}
