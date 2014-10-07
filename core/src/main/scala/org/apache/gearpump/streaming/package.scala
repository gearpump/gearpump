package org.apache.gearpump

import org.apache.gearpump.streaming.task.TaskId
import org.apache.gearpump.util.Configs
import org.apache.gearpump.util.Constants._

package object streaming {
  type TaskGroup = Int
  type TaskIndex = Int

  class ConfigsHelper(config : Configs) {
    def withTaskId(taskId : TaskId) =  config.withValue(TASK_ID, taskId)
    def taskId : TaskId = config.getAnyRef(TASK_ID).asInstanceOf[TaskId]

    def withDag(taskDag : DAG) = config.withValue(TASK_DAG, taskDag)
    def dag : DAG = config.getAnyRef(TASK_DAG).asInstanceOf[DAG]
  }

  object ConfigsHelper {
    implicit def toConfigHelper(config: Configs) = new ConfigsHelper(config)
  }
}
