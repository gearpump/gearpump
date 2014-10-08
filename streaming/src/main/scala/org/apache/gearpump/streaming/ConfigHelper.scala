package org.apache.gearpump.streaming

import java.util

import akka.actor.Actor
import com.typesafe.config.Config
import org.apache.gearpump.streaming.task.TaskId
import org.apache.gearpump.util.{Constants, Configs}
import org.apache.gearpump.util.Constants._

class ConfigsHelper(config : Configs) {
  def withTaskId(taskId : TaskId) =  config.withValue(TASK_ID, taskId)
  def taskId : TaskId = config.getAnyRef(TASK_ID).asInstanceOf[TaskId]

  def withDag(taskDag : DAG) = config.withValue(TASK_DAG, taskDag)
  def dag : DAG = config.getAnyRef(TASK_DAG).asInstanceOf[DAG]
}

object ConfigsHelper {
  implicit def toConfigHelper(config: Configs) = new ConfigsHelper(config)

  def loadUserAllocation(config: Config) : Array[(Int, TaskDescription)] ={
    import scala.collection.JavaConverters._
    var result = new Array[(Int, TaskDescription)](0)
    if(!config.hasPath(Constants.GEARPUMP_SCHEDULING_REQUEST))
      return result
    val allocations = config.getObject(Constants.GEARPUMP_SCHEDULING_REQUEST)
    if(allocations == null)
      return result
    for(worker <- allocations.keySet().asScala.toSet[String]){
      val tasks = allocations.get(worker).unwrapped().asInstanceOf[util.HashMap[String, Object]]
      for( taskClass <- tasks.keySet().asScala.toSet[String]){
        val taskClazz = Class.forName(taskClass).asInstanceOf[Class[Actor]]
        val parallism = tasks.get(taskClass).asInstanceOf[Int]
        result = result :+ (worker.toInt, TaskDescription(taskClazz, parallism))
      }
    }
    result
  }
}
