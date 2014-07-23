package org.apache.gearpump

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import org.apache.gearpump.service.SimpleKVService
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.Queue

/**
 * Created by xzhong10 on 2014/7/22.
 */
class AppMaster() extends Actor {
  import AppMaster._

  private var master : ActorRef = null
  private var executorCount : Int = 0;
  private var appId : Int = 0;
  def receive : Receive = clientMsgHandler

  def clientMsgHandler : Receive = {
    case SubmitApplication(app) =>
      LOG.info("Application submitted " + app.name)
      //new Actor, actor handle Application
      val client = sender;
      val application = context.actorOf(Props(new Application(appId, client, app)), appId.toString)
      appId += 1
  }

  override def preStart() : Unit = {
    master = ActorUtil.getMaster(context.system)

    val appMasterPath = ActorUtil.getFullPath(context)
    SimpleKVService.set("appMaster", appMasterPath)
  }

  class Application (appId : Int, client : ActorRef, appDescription : AppDescription) extends Actor {
    private val name = appDescription.name
    private val taskQueue = new Queue[(Int, TaskDescription, Range)]

    override def preStart : Unit = {
      var stageCount = appDescription.stages.length
      var stageFirstTaskId = appDescription.stages.foldLeft(Array(0))((array, stage) => array :+ (array.last + stage.parallism))

      val tasks = appDescription.stages.zipWithIndex.flatMap((stageInfo) => {
        val (stage, stageId) = stageInfo
        0.until(stage.parallism).map((seq : Int) => {
          val sortOrder = seq * stageCount + stageId
          val taskId = stageFirstTaskId(stageId) + seq
          val nextStageId = stageId + 1
          val nextStageRange = if(nextStageId < stageCount) {
            Range(stageFirstTaskId(nextStageId), stageFirstTaskId(nextStageId + 1))
          } else {
            Range(stageFirstTaskId(nextStageId), stageFirstTaskId(nextStageId))
          }
          ((taskId, stage.task, nextStageRange), sortOrder)
        })}).sortBy(_._2).map(_._1)

      taskQueue ++= tasks
       master ! RequestResource(taskQueue.size)
   }

    override def receive : Receive = masterMsgHandler orElse  workerMsgHandler orElse  executorMsgHandler

    def masterMsgHandler : Receive = {
      case ResourceAllocated(resource) => {
        LOG.info("Resource allocated " + resource.toString)
        //group resource by worker
        val groupedResource = resource.groupBy(_.worker).mapValues(_.foldLeft(0)((count, resource) => count + resource.slots)).toArray

        var executorId = 0;
        groupedResource.map((workerAndSlots) => {
          val (worker, slots) = workerAndSlots
          LOG.info("Launching Executor ...appId: " + appId + ", executorId: " + executorId + ", slots: " + slots + " on worker " + worker.toString())
          worker ! LaunchExecutor(appId, executorId, slots, new DefaultExecutorContext)
          //TODO: Add timeout event if this executor fail to start
          executorId += 1
        })
      }
    }

    def executorMsgHandler : Receive = {
      case TaskLaunched(taskId) =>
        LOG.info("Task Launched " + taskId)
      case TaskSuccess =>
      case TaskFailed(taskId, reason, ex) =>
        LOG.info("Task failed, taskId: " + taskId)
    }

    def workerMsgHandler : Receive = {
      case ExecutorLaunched(executor, executorId, slots) => {
        def launchTask(remainSlots: Int): Unit = {
          if (remainSlots > 0 && !taskQueue.isEmpty) {
            val (taskId, taskDescription, range) = taskQueue.dequeue()
            //Launch task
            executor ! LaunchTask(taskId, taskDescription, range)
            launchTask(remainSlots - 1)
          }
        }
        launchTask(slots)
      }
      case ExecutorLaunchFailed(executorId, reason, _) => {

        //TODO: error handling
      }

      //  Unit
      //TODO
      //handle resource allocation
      //ask to create executor
      //receive message about executor launched
      //get exeutor actor
      //create task
      //wait for stop message from client
      //Or task complete message
      //If all task completed
      //Then shutdown
    }

  }
}

object AppMaster {
  private val LOG: Logger = LoggerFactory.getLogger(AppMaster.getClass)

  def main (args: Array[String]) {

    val kvService = args(0);
    SimpleKVService.init(kvService)

    print("Starting AppMaster ...")

    val system = ActorSystem("executor", Configs.SYSTEM_DEFAULT_CONFIG)

    LOG.info("executor process is started...");


    val actor = system.actorOf(Props(classOf[AppMaster]), "appMaster")


    system.awaitTermination()
    LOG.info("executor process is shutdown...");
  }
}
