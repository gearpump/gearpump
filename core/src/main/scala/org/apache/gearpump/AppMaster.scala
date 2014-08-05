package org.apache.gearpump
import org.apache.gearpump.util.{DAG, Graph}
import akka.actor.{Actor, ActorRef, Terminated}
import org.apache.gearpump.task.TaskId
import org.apache.gears.cluster.AppMasterToExecutor._
import org.apache.gears.cluster.AppMasterToMaster._
import org.apache.gears.cluster.AppMasterToWorker._
import org.apache.gears.cluster.ExecutorToAppMaster._
import org.apache.gears.cluster.ExecutorToWorker.RegisterExecutor
import org.apache.gears.cluster.MasterToAppMaster._
import org.apache.gears.cluster.WorkerToAppMaster._
import org.apache.gears.cluster._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.Queue
import org.jgrapht.traverse.TopologicalOrderIterator;
import scala.collection.JavaConversions._
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class AppMaster (config : Configs) extends Actor {
  import org.apache.gearpump.AppMaster._

  val masterExecutorId = config.executorId
  var currentExecutorId = masterExecutorId + 1
  val slots = config.slots

  private val appId = config.appId
  private val appDescription = config.appDescription.asInstanceOf[AppDescription]
  private val master = config.master
  private val appManager = config.appManager

  private val name = appDescription.name
  private val taskQueue = new Queue[(TaskId, TaskDescription, DAG)]
  private var taskLocations = Map[TaskId, ActorRef]()
  private var pendingTaskLocationQueries = new mutable.HashMap[TaskId, mutable.ListBuffer[ActorRef]]()


  override def preStart : Unit = {
    context.parent ! RegisterExecutor(appManager, appId, masterExecutorId, slots)

    LOG.info(s"AppMaster[$appId] is launched $appDescription")
    Console.out.println("AppMaster is launched xxxxxxxxxxxxxxxxx")

    val dag = DAG(appDescription.dag)

    //scheduler the task fairly on every machine
    val tasks = dag.tasks.flatMap { stageInfo =>
      val (stageId, stage) = stageInfo
      0.until(stage.parallism).map((taskIndex : Int) => {
        val taskId = TaskId(stageId, taskIndex)
        val nextStageId = stageId + 1
        (taskId, stage, dag.subGraph(stageId))
      })}.toArray.sortBy(_._1.index)

    taskQueue ++= tasks
    LOG.info(s"App Master $appId request Resource ${taskQueue.size}")
    master ! RequestResource(appId, taskQueue.size)
  }

  override def receive : Receive = masterMsgHandler orElse  workerMsgHandler orElse  executorMsgHandler orElse terminationWatch

  def masterMsgHandler : Receive = {
    case ResourceAllocated(resource) => {
      LOG.info(s"AppMaster $appId received ResourceAllocated $resource")
      //group resource by worker
      val groupedResource = resource.groupBy(_.worker).mapValues(_.foldLeft(0)((count, resource) => count + resource.slots)).toArray

      groupedResource.map((workerAndSlots) => {
        val (worker, slots) = workerAndSlots
        LOG.info(s"Launching Executor ...appId: $appId, executorId: $currentExecutorId, slots: $slots on worker $worker")
        val executorConfig = appDescription.conf.withAppId(appId).withAppMaster(self).withExecutorId(currentExecutorId).withSlots(slots)
        worker ! LaunchExecutor(appId, currentExecutorId, slots,  classOf[Executor], executorConfig, new DefaultExecutorContext)
        //TODO: Add timeout event if this executor fail to start
        currentExecutorId += 1
      })
    }
  }

  def executorMsgHandler : Receive = {
    case TaskLaunched(taskId, task) =>
      LOG.info(s"Task $taskId has been Launched for app $appId")
      taskLocations += taskId -> task
      pendingTaskLocationQueries.get(taskId).map((list) => list.foreach(_ ! TaskLocation(taskId, task)))
      pendingTaskLocationQueries.remove(taskId)
    case GetTaskLocation(taskId) => {
      LOG.info(s"Get Task $taskId Location for app $appId")
      if (taskLocations.get(taskId).isDefined) {
        LOG.info(s"App: $appId, We got a location for task $taskId, sending back directly  ")
        sender ! TaskLocation(taskId, taskLocations.get(taskId).get)
      } else {
        LOG.info(s"App[$appId] We don't have the task location right now, add to a pending list... ")
        val pendingQueries = pendingTaskLocationQueries.getOrElseUpdate(taskId, mutable.ListBuffer[ActorRef]())
        pendingQueries += sender
      }
    }
    case TaskSuccess =>
    case TaskFailed(taskId, reason, ex) =>
      LOG.info(s"Task failed, taskId: $taskId for app $appId")
  }

  def workerMsgHandler : Receive = {
    case ExecutorLaunched(executor, executorId, slots) => {
      LOG.info(s"executor $executorId has been launched")
      //watch for executor termination
      context.watch(executor)

      def launchTask(remainSlots: Int): Unit = {
        if (remainSlots > 0 && !taskQueue.isEmpty) {
          val (taskId, taskDescription, dag) = taskQueue.dequeue()
          //Launch task

          LOG.info("Sending Launch Task to executor: " + executor.toString())

          val executorByPath = context.actorSelection("../app_0_executor_0")

          val config = appDescription.conf.withAppId(appId).withExecutorId(executorId).withAppMaster(self).withDag(dag)
          executor ! LaunchTask(taskId, config, taskDescription.taskClass)
          launchTask(remainSlots - 1)
        }
      }
      launchTask(slots)
    }
    case ExecutorLaunchFailed(launch, reason, ex) => {
      LOG.error(s"Executor Launch failed $launch, reason：$reason", ex)
    }
  }

  def terminationWatch : Receive = {
    case Terminated(actor) => Unit
  }
}

object AppMaster {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[AppMaster])

  case class TaskData(taskDescription : TaskDescription, dag : DAG) extends Task
}