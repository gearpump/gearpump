package io.gearpump.cluster.appmaster

import akka.actor.ActorRef
import io.gearpump.cluster.worker.WorkerId

case class WorkerInfo(workerId: WorkerId, ref: ActorRef)
