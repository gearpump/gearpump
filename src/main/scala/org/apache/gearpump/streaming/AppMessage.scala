package org.apache.gearpump.streaming

import akka.actor.Actor
import org.apache.gearpump.cluster.Configs
import org.apache.gearpump.streaming.task.TaskId
import org.apache.gearpump.transport.ExpressAddress

object AppMasterToExecutor {
  case class LaunchTask(taskId: TaskId, config : Configs, taskClass: Class[_ <: Actor])
  case class TaskLocation(taskId: TaskId, task: ExpressAddress)
}

object ExecutorToAppMaster {
  case class TaskLaunched(taskId: TaskId, task: ExpressAddress)
  case object TaskSuccess
  case class TaskFailed(taskId: TaskId, reason: String = null, ex: Exception = null)
  case class GetTaskLocation(taskId: TaskId)
}