/*
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

package org.apache.gearpump.streaming

import akka.actor._
import akka.remote.RemoteScope
import org.apache.gearpump.cluster.AppMasterToMaster._
import org.apache.gearpump.cluster.AppMasterToWorker._
import org.apache.gearpump.cluster.MasterToAppMaster._
import org.apache.gearpump.cluster.WorkerToAppMaster._
import org.apache.gearpump.cluster._
import org.apache.gearpump.streaming.AppMasterToExecutor.LaunchTask
import org.apache.gearpump.streaming.ExecutorToAppMaster._
import org.apache.gearpump.streaming.task.{TaskId, TaskLocations}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.ActorSystemBooter.{BindLifeCycle, RegisterActorSystem}
import org.apache.gearpump.util.{ActorSystemBooter, ActorUtil, DAG, Util}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.Queue
class AppMaster (config : Configs) extends Actor {

  import org.apache.gearpump.streaming.AppMaster._

  val masterExecutorId = config.executorId
  var currentExecutorId = masterExecutorId + 1
  val slots = config.slots

  private val appId = config.appId
  private val appDescription = config.appDescription.asInstanceOf[AppDescription]
  private val master = config.master
  private val appManager = config.appManager

  private val name = appDescription.name
  private val taskQueue = new Queue[(TaskId, TaskDescription, DAG)]

  private var taskLocations = Map.empty[HostPort, Set[TaskId]]

  private var startedTasks = Set.empty[TaskId]
  private var totalTaskCount = 0

  override def preStart: Unit = {
    context.parent ! RegisterMaster(appManager, appId, masterExecutorId, slots)

    LOG.info(s"AppMaster[$appId] is launched $appDescription")
    Console.out.println("AppMaster is launched xxxxxxxxxxxxxxxxx")

    val dag = DAG(appDescription.dag)

    //scheduler the task fairly on every machine
    val tasks = dag.tasks.flatMap { params =>
      val (taskGroupId, taskDescription) = params
      0.until(taskDescription.parallism).map((taskIndex: Int) => {
        val taskId = TaskId(taskGroupId, taskIndex)
        val nextGroupId = taskGroupId + 1
        (taskId, taskDescription, dag.subGraph(taskGroupId))
      })
    }.toArray.sortBy(_._1.index)

    totalTaskCount = tasks.size
    taskQueue ++= tasks
    LOG.info(s"App Master $appId request Resource ${taskQueue.size}")
    master ! RequestResource(appId, taskQueue.size)
  }

  override def receive: Receive = masterMsgHandler orElse selfMsgHandler orElse workerMsgHandler orElse executorMsgHandler orElse terminationWatch

  def masterMsgHandler: Receive = {
    case ResourceAllocated(resource) => {
      LOG.info(s"AppMaster $appId received ResourceAllocated $resource")
      //group resource by worker
      val groupedResource = resource.groupBy(_.worker).mapValues(_.foldLeft(0)((count, resource) => count + resource.slots)).toArray

      groupedResource.map((workerAndSlots) => {
        val (worker, slots) = workerAndSlots
        LOG.info(s"Launching Executor ...appId: $appId, executorId: $currentExecutorId, slots: $slots on worker $worker")
        val executorConfig = appDescription.conf.withAppId(appId).withAppMaster(self).withExecutorId(currentExecutorId).withSlots(slots)


        context.actorOf(Props(classOf[ExecutorLauncher], worker, appId, currentExecutorId, slots, executorConfig))
        currentExecutorId += 1
      })
    }
  }

  def executorMsgHandler: Receive = {
    case TaskLaunched(taskId, host) => {
      LOG.info(s"Task $taskId has been Launched for app $appId")

      var taskIds = taskLocations.get(host).getOrElse(Set.empty[TaskId])
      taskIds += taskId
      taskLocations += host -> taskIds
      startedTasks += taskId

      LOG.info(s" started task size: ${startedTasks.size}, taskQueue size: ${totalTaskCount}")
      if (startedTasks.size == totalTaskCount) {
        context.children.foreach { executor =>
          LOG.info(s"Sending Task locations to executor ${executor.path.name}")
          executor ! TaskLocations(taskLocations)
        }
      }
    }
    case task: TaskFinished => {
      val taskId = task.taskId
      LOG.info(s"Task ${taskId} has been finished for app $appId")

      taskLocations.keys.foreach { host =>
        if (taskLocations.contains(host)) {
          var set = taskLocations.get(host).get
          if (set.contains(taskId)) {
            set -= taskId
            taskLocations += host -> set
          }
        }
      }

      task match {
        case TaskSuccess(taskId) => Unit
        case TaskFailed(taskId, reason, ex) =>
          LOG.info(s"Task failed, taskId: $taskId for app $appId")
          //TODO: Reschedule the task to other nodes
      }
    }
  }

  def selfMsgHandler : Receive = {
    case LaunchExecutorActor(conf : Props, executorId : Int, daemon : ActorRef) =>
      val executor = context.actorOf(conf, executorId.toString)
      daemon ! BindLifeCycle(executor)
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
    case ExecutorLaunchFailed(reason, ex) => {
      LOG.error(s"Executor Launch failed reason：$reason", ex)
    }
  }

  //TODO: We an task is down, we need to recover
  def terminationWatch : Receive = {
    case Terminated(actor) => Unit
  }
}

object AppMaster {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[AppMaster])

  case class TaskData(taskDescription : TaskDescription, dag : DAG)

  class ExecutorLauncher (worker : ActorRef, appId : Int, executorId : Int, slots : Int, executorConfig : Configs) extends Actor {

    private def actorNameForExecutor(appId : Int, executorId : Int) = "app" + appId + "-executor" + executorId

    val name = actorNameForExecutor(appId, executorId)
    val selfPath = ActorUtil.getFullPath(context)

    val launch = ExecutorContext(Util.getCurrentClassPath, context.system.settings.config.getString("gearpump.streaming.executor.vmargs").split(" "), classOf[ActorSystemBooter].getName, Array(name, selfPath))

    worker ! LaunchExecutor(appId, executorId,slots, launch)

    def receive : Receive = waitForActorSystemToStart


    def waitForActorSystemToStart : Receive = {
      case RegisterActorSystem(systemPath) =>
        LOG.info(s"Received RegisterActorSystem $systemPath for app master")
        val executorProps = Props(classOf[Executor], executorConfig).withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(systemPath))))
        sender ! BindLifeCycle(worker)
        context.parent ! LaunchExecutorActor(executorProps, executorConfig.executorId, sender)
        context.stop(self)
    }
  }

  case class LaunchExecutorActor(executorConfig : Props, executorId : Int, daemon: ActorRef)
}