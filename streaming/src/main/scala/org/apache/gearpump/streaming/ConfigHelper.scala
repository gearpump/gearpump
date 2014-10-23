package org.apache.gearpump.streaming

import java.util

import akka.actor.Actor
import com.typesafe.config.Config
import org.apache.gearpump.streaming.task.TaskId
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.{Configs, Constants}

class ConfigsHelper(config : Configs) {
  def withTaskId(taskId : TaskId) =  config.withValue(TASK_ID, taskId)
  def taskId : TaskId = config.getAnyRef(TASK_ID).asInstanceOf[TaskId]

  def withDag(taskDag : DAG) = config.withValue(TASK_DAG, taskDag)
  def dag : DAG = config.getAnyRef(TASK_DAG).asInstanceOf[DAG]
}

object ConfigsHelper {
  implicit def toConfigHelper(config: Configs) = new ConfigsHelper(config)

  /*
  The task resource requests format:
  gearpump {
    scheduling {
      requests {
        worker1 {
          "task1" = 2   //parallelism
          "task2" = 4
        }
        worker2 {
          "task1" = 2
        }
      }
    }
  }
   */
  def loadUserAllocation(config: Config) : Array[(TaskDescription, Locality)] ={
    import scala.collection.JavaConverters._
    var result = new Array[(TaskDescription, Locality)](0)
    if(!config.hasPath(Constants.GEARPUMP_SCHEDULING_REQUEST))
      return result
    val requests = config.getObject(Constants.GEARPUMP_SCHEDULING_REQUEST)
    for(workerId <- requests.keySet().asScala.toSet[String]){
      val taskDescriptions = requests.get(workerId).unwrapped().asInstanceOf[util.HashMap[String, Int]].asScala
      for(taskDescription <- taskDescriptions){
        val (taskClass, parallism) = taskDescription
        val taskClazz = Class.forName(taskClass).asInstanceOf[Class[Actor]]
        result = result :+ (TaskDescription(taskClazz, parallism), WorkerLocality(workerId.toInt))
      }
    }
    result
  }
}
