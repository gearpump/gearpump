package org.apache.gearpump
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import org.apache.gearpump.service.SimpleKVService
import org.apache.gearpump.task.TaskId
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable
import scala.concurrent.Promise

import scala.collection.mutable.Queue


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
    private val taskQueue = new Queue[(TaskId, TaskDescription, StageParallism)]
    private var taskLocations = Map[TaskId, ActorRef]()
    private var pendingTaskLocationQueries = new mutable.HashMap[TaskId, mutable.ListBuffer[ActorRef]]()

    override def preStart : Unit = {
      val stageParallisms = appDescription.stages.map(_.parallism).zipWithIndex.map((pair) => StageParallism(pair._2, pair._1)) :+ StageParallism(-1, 0)


      val tasks = appDescription.stages.zipWithIndex.flatMap((stageInfo) => {
        val (stage, stageId) = stageInfo
        0.until(stage.parallism).map((taskIndex : Int) => {
          val taskId = TaskId(stageId, taskIndex)
          val nextStageId = stageId + 1
          val nextStageParallism = if (nextStageId >= stageParallisms.length) null else stageParallisms(nextStageId)
          (taskId, stage.task, nextStageParallism)
        })}).sortBy(_._1.index)

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
      case TaskLaunched(taskId, task) =>
        LOG.info("Task Launched " + taskId)
        taskLocations += taskId -> task
        pendingTaskLocationQueries.get(taskId).map((list) => list.foreach(_ ! TaskLocation(taskId, task)))
        pendingTaskLocationQueries.remove(taskId)
      case GetTaskLocation(taskId) => {
        LOG.info("Get Task Location " + taskId)
        if (taskLocations.get(taskId).isDefined) {
          LOG.info("We got a location, sending back directly  " + taskId)
          sender ! TaskLocation(taskId, taskLocations.get(taskId).get)
        } else {
          LOG.info("We don't have it right now, add to a pending list... ")
          val pendingQueries = pendingTaskLocationQueries.getOrElseUpdate(taskId, mutable.ListBuffer[ActorRef]())
          pendingQueries += sender
        }
      }
      case TaskSuccess =>
      case TaskFailed(taskId, reason, ex) =>
        LOG.info("Task failed, taskId: " + taskId)
    }

    def workerMsgHandler : Receive = {
      case ExecutorLaunched(executor, executorId, slots) => {
        def launchTask(remainSlots: Int): Unit = {
          if (remainSlots > 0 && !taskQueue.isEmpty) {
            val (taskId, taskDescription, nextStageParallism) = taskQueue.dequeue()
            //Launch task
            executor ! LaunchTask(taskId, appDescription.conf, taskDescription, nextStageParallism)
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
