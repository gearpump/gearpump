package org.apache.gearpump.streaming

import akka.actor.Actor
import org.apache.gearpump.cluster.Configs
import org.apache.gearpump.streaming.task.TaskId
import org.apache.gearpump.transport.{HostPort}

object AppMasterToExecutor {
  case class LaunchTask(taskId: TaskId, config : Configs, taskClass: Class[_ <: Actor])
}

object ExecutorToAppMaster {
  case class TaskLaunched(taskId: TaskId, task: HostPort)

  trait TaskFinished {
    def taskId : TaskId
  }

  case class TaskSuccess(taskId : TaskId) extends TaskFinished
  case class TaskFailed(taskId: TaskId, reason: String = null, ex: Exception = null) extends TaskFinished
}