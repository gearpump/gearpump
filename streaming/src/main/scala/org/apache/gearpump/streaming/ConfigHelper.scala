package org.apache.gearpump.streaming

import java.util

import com.typesafe.config.Config
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{TaskContext, TaskId}
import org.apache.gearpump.util.Constants
import scala.language.implicitConversions

class ConfigsHelper(config : UserConfig) {

}

object ConfigsHelper {
  implicit def toConfigHelper(config: UserConfig) = new ConfigsHelper(config)

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
        result = result :+ (TaskDescription(taskClass, parallism), WorkerLocality(workerId.toInt))
      }
    }
    result
  }
}
