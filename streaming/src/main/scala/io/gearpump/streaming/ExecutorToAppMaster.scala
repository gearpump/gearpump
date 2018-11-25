package io.gearpump.streaming

import akka.actor.ActorRef
import io.gearpump.cluster.appmaster.WorkerInfo
import io.gearpump.cluster.scheduler.Resource
import io.gearpump.streaming.task.TaskId
import io.gearpump.transport.HostPort

object ExecutorToAppMaster {
  case class RegisterExecutor(
      executor: ActorRef, executorId: Int, resource: Resource, worker : WorkerInfo)

  case class RegisterTask(taskId: TaskId, executorId: Int, task: HostPort)
  case class UnRegisterTask(taskId: TaskId, executorId: Int)

  case class MessageLoss(executorId: Int, taskId: TaskId,
      cause: String, ex: Option[Throwable] = None)
}
