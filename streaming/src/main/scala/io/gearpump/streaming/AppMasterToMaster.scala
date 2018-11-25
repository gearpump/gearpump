package io.gearpump.streaming

import io.gearpump.streaming.task.TaskId

object AppMasterToMaster {
  case class StallingTasks(tasks: List[TaskId])
}
