package io.gearpump.cluster.appmaster

import akka.actor.{ActorRef, Address, PoisonPill}
import io.gearpump.util.ActorSystemBooter.BindLifeCycle
import io.gearpump.cluster.scheduler.Resource

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
