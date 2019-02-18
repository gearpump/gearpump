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

package io.gearpump.cluster.appmaster

import akka.actor.{ActorRef, Address, PoisonPill}
import io.gearpump.cluster.scheduler.Resource
import io.gearpump.util.ActorSystemBooter.BindLifeCycle

/**
 * Configurations to start an executor system on remote machine
 *
 * @param address Remote address where we start an Actor System.
 */
case class ExecutorSystem(executorSystemId: Int, address: Address, daemon:
    ActorRef, resource: Resource, worker: WorkerInfo) {
  def bindLifeCycleWith(actor: ActorRef): Unit = {
    daemon ! BindLifeCycle(actor)
  }

  def shutdown(): Unit = {
    daemon ! PoisonPill
  }
}
