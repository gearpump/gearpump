package org.apache.gearpump

import akka.actor.Actor
import org.apache.gearpump.task.TaskId
import org.apache.gearpump.transport.ExpressAddress
import org.apache.gears.cluster.Configs

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