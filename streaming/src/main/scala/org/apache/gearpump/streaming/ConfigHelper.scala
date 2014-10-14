package org.apache.gearpump.streaming

import org.apache.gearpump.streaming.task.TaskId
import org.apache.gearpump.util.Configs
import org.apache.gearpump.util.Constants._
import org.apache.gearpump._

class ConfigsHelper(config : Configs) {
  def withTaskId(taskId : TaskId) =  config.withValue(TASK_ID, taskId)
  def taskId : TaskId = config.getAnyRef(TASK_ID).asInstanceOf[TaskId]

  def withDag(taskDag : DAG) = config.withValue(TASK_DAG, taskDag)
  def dag : DAG = config.getAnyRef(TASK_DAG).asInstanceOf[DAG]

  def withStartTime(time : TimeStamp) = config.withValue(START_TIME, time)
  def startTime = config.getLong(START_TIME)
}

object ConfigsHelper {
  implicit def toConfigHelper(config: Configs) = new ConfigsHelper(config)
}
