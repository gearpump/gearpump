package org.apache.gearpump.streaming

import akka.actor.{Actor, ActorRef, Kill}
import org.apache.gearpump.streaming.AppMasterToController.{AllTaskLaunched, ExecutorFailed, TaskAdded, TaskLaunched}
import org.apache.gearpump.streaming.Controller.TaskInfo
import org.apache.gearpump.streaming.ControllerToAppMaster.ReScheduleFailedTasks
import org.apache.gearpump.streaming.ControllerToTask.ResetReaderOffset
import org.apache.gearpump.streaming.task.{GetLatestMinClock, LatestMinClock, TaskId}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class Controller(appDescription : AppDescription) extends Actor{
  private val LOG: Logger = LoggerFactory.getLogger(classOf[Controller])
  private var recovering = false;

  val executorWithTasks = new mutable.HashMap[ActorRef, Array[TaskInfo]]()
  override def receive: Receive = appMasterMsgHandler

  def appMasterMsgHandler : Receive = {
    case TaskAdded(executor, taskId, taskClass) => {
      var taskArray = Array.empty[TaskInfo]
      if(executorWithTasks.contains(executor)){
        taskArray = executorWithTasks.get(executor).get
      }
      taskArray = taskArray :+ TaskInfo(taskId, taskClass)
      executorWithTasks.put(executor, taskArray)
    }
    case TaskLaunched(taskId, taskRef) => {
      val task = executorWithTasks.values.flatMap(_.toList).find(_.taskId.equals(taskId))
      task match {
        case Some(taskInfo) => taskInfo.actorRef = taskRef
      }
    }
    case AllTaskLaunched => {
      if(recovering) {
        sender() ! GetLatestMinClock
      }
    }
    case LatestMinClock(timeStamp) =>{
      val spout = appDescription.dag.topologicalOrderIterator.next()
      executorWithTasks.values.flatMap(_.toList).filter(_.taskClass.equals(spout.taskClass)).foreach(_.actorRef ! ResetReaderOffset(timeStamp))
      recovering = false;
      context.become(appMasterMsgHandler)
    }
    case ExecutorFailed(executor) => {
      if(!executorWithTasks.contains(executor)){
        //TODO: Exception handle
      }
      executorWithTasks.keySet.toArray.filter(!_.equals(executor)).foreach(_ ! Kill)
      val tasks : Array[(TaskId, Class[_ <: Actor])]= executorWithTasks.get(executor).get.map(params => (params.taskId, params.taskClass))
      executorWithTasks -= executor
      sender() ! ReScheduleFailedTasks(tasks)
      recovering = true;
    }
  }
}

object Controller{
  case class TaskInfo(taskId: TaskId, taskClass: Class[_ <: Actor], var actorRef: ActorRef = null)
}
