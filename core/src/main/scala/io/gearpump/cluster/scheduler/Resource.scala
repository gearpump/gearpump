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

import akka.actor.ActorRef
import io.gearpump.cluster.scheduler.Priority.NORMAL
import io.gearpump.cluster.scheduler.Priority.Priority
import io.gearpump.cluster.scheduler.Relaxation.ANY
import io.gearpump.cluster.scheduler.Relaxation.Relaxation
import io.gearpump.cluster.worker.WorkerId

case class Resource(slots: Int) {

  // scalastyle:off spaces.after.plus
  def +(other: Resource): Resource = Resource(slots + other.slots)
  // scalastyle:on spaces.after.plus

  def -(other: Resource): Resource = Resource(slots - other.slots)

  def >(other: Resource): Boolean = slots > other.slots

  def >=(other: Resource): Boolean = !(this < other)

  def <(other: Resource): Boolean = slots < other.slots

  def <=(other: Resource): Boolean = !(this > other)

  def isEmpty: Boolean = {
    slots == 0
  }
}

/**
 * Each streaming job can have a priority, the job with higher priority
 * will get scheduled resource earlier than those with lower priority.
 */
object Priority extends Enumeration {
  type Priority = Value
  val LOW, NORMAL, HIGH = Value
}

/**
 * Relaxation.ONEWORKER means only resource (slot) from that worker will be accepted by
 * the requestor application job.
 */
object Relaxation extends Enumeration {
  type Relaxation = Value

  // Option ONEWORKER allow user to schedule a task on specific worker.
  val ANY, ONEWORKER, SPECIFICWORKER = Value
}

case class ResourceRequest(
    resource: Resource, workerId: WorkerId, priority: Priority = NORMAL,
    relaxation: Relaxation = ANY, executorNum: Int = 1)

case class ResourceAllocation(resource: Resource, worker: ActorRef, workerId: WorkerId)

object Resource {
  def empty: Resource = new Resource(0)

  def min(res1: Resource, res2: Resource): Resource = if (res1.slots < res2.slots) res1 else res2
}

