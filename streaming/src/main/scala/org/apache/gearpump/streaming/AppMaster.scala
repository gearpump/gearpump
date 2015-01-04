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

import java.io.{ByteArrayOutputStream, File, FileInputStream}
import java.util.Date
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern.{ask, pipe}
import akka.remote.RemoteScope
import org.apache.gearpump._
import org.apache.gearpump.cluster.AppMasterToMaster._
import org.apache.gearpump.cluster.AppMasterToWorker._
import org.apache.gearpump.cluster.ClientToMaster.ShutdownApplication
import org.apache.gearpump.cluster.MasterToAppMaster._
import org.apache.gearpump.cluster.WorkerToAppMaster._
import org.apache.gearpump.cluster._
import org.apache.gearpump.cluster.scheduler._
import org.apache.gearpump.streaming.AppMasterToExecutor.{StartClock, LaunchTask, RestartTasks}
import org.apache.gearpump.streaming.ConfigsHelper._
import org.apache.gearpump.streaming.ExecutorToAppMaster._
import org.apache.gearpump.streaming.storage.{InMemoryAppStoreOnMaster, AppDataStore}
import org.apache.gearpump.streaming.task._
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.ActorSystemBooter.{BindLifeCycle, RegisterActorSystem}
import org.apache.gearpump.util._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent._
import scala.concurrent.duration.{Duration, FiniteDuration}

class AppMaster(appId : Int, username : String, masterExecutorId : Int, resource : Resource,
                 appDescription : AppDescription, appJar : Option[AppJar], masterProxy : ActorRef,
                 registerData : AppMasterRegisterData, config : Configs) extends ApplicationMaster {

  def this(config : Configs)  = {
    this(config.appId, config.username, config.executorId, config.resource,
      config.appDescription.asInstanceOf[AppDescription], config.appjar,
      config.masterProxy, config.appMasterRegisterData, config)
  }

  import org.apache.gearpump.streaming.AppMaster._
  implicit val timeout = Constants.FUTURE_TIMEOUT

  var currentExecutorId = masterExecutorId + 1

  import context.dispatcher

  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)

  private var master : ActorRef = null



  private val name = appDescription.name
  private val taskSet = new TaskSet(config, DAG(appDescription.dag))

  private var clockService : ActorRef = null
  private val START_CLOCK = "startClock"
  private var startClock : TimeStamp = 0L

  private var taskLocations = Map.empty[HostPort, Set[TaskId]]
  private var executorIdToTasks = Map.empty[Int, Set[TaskId]]

  private var startedTasks = Set.empty[TaskId]
  private var updateScheduler : Cancellable = null
  private var store : AppDataStore = null
  private var allocationTimeOut: Cancellable = null
  //When the AppMaster trying to replay, the replay command should not be handled again.
  private var restarting = true

  override def receive : Receive = null

  override def preStart(): Unit = {
    LOG.info(s"AppMaster[$appId] is launched by $username $appDescription")
    updateScheduler = context.system.scheduler.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
      new FiniteDuration(5, TimeUnit.SECONDS))(snapshotStartClock())

    val dag = DAG(appDescription.dag)

    LOG.info("AppMaster is launched xxxxxxxxxxxxxxxxx")

    LOG.info("Initializing Clock service ....")
    clockService = context.actorOf(Props(classOf[ClockService], dag), "clockservice")

    context.become(waitForMasterToConfirmRegistration(repeatActionUtil(30)(masterProxy ! RegisterAppMaster(self, appId, masterExecutorId, resource, registerData))))
  }

  def waitForMasterToConfirmRegistration(killSelf : Cancellable) : Receive = {
    case AppMasterRegistered(appId, master) =>
      LOG.info(s"AppMasterRegistered received for appID: $appId")
      killSelf.cancel()
      this.master = master
      context.watch(master)
      store = new InMemoryAppStoreOnMaster(appId, master)

      LOG.info(s"try to recover start clock")
      store.get(START_CLOCK).map{clock =>
        if(clock != null){
          startClock = clock.asInstanceOf[TimeStamp]
          LOG.info(s"recover start clock sucessfully and the start clock is ${new Date(startClock)}")
        }
        LOG.info("Sending request resource to master...")
        val resourceRequests = taskSet.fetchResourceRequests()
        resourceRequests.foreach(master ! RequestResource(appId, _))
        context.become(messageHandler)
      }
  }

  def messageHandler: Receive = masterMsgHandler orElse selfMsgHandler orElse appManagerMsgHandler orElse workerMsgHandler orElse executorMsgHandler orElse terminationWatch

  def masterMsgHandler: Receive = {
    case ResourceAllocated(allocations) =>
      if (null != allocationTimeOut) {
        allocationTimeOut.cancel()
      }
      if (!enoughResourcesAllocated(allocations)) {
        allocationTimeOut = context.system.scheduler.scheduleOnce(Duration(30, TimeUnit.SECONDS), self, AllocateResourceTimeOut)
      }
      val actorToWorkerId = mutable.HashMap.empty[ActorRef, Int]
      val groupedResource = allocations.groupBy(_.worker).mapValues(_.foldLeft(Resource.empty)((totalResource, request) => totalResource add request.resource)).toArray
      allocations.foreach(allocation => actorToWorkerId.put(allocation.worker, allocation.workerId))

      groupedResource.map((workerAndResources) => {
        val (worker, resource) = workerAndResources
        LOG.info(s"Launching Executor ...appId: $appId, executorId: $currentExecutorId, slots: ${resource.slots} on worker $worker")
        val executorConfig = appDescription.conf.withAppId(appId).withUserName(username).withAppMaster(self).withExecutorId(currentExecutorId).withResource(resource).withStartTime(startClock).withWorkerId(actorToWorkerId.get(worker).get)
        context.actorOf(Props(classOf[ExecutorLauncher], worker, appId, currentExecutorId, resource, executorConfig, appJar), s"launcher${currentExecutorId}")
        currentExecutorId += 1
      })
    case AllocateResourceTimeOut =>
      if (startedTasks.size < taskSet.totalTaskCount) {
        LOG.error(s"AppMaster did not receive enough resource to launch ${taskSet.size} tasks, " +
          s"shutting down the application...")
        master ! ShutdownApplication(appId)
      }
  }

  private def enoughResourcesAllocated(allocations: Array[ResourceAllocation]): Boolean = {
    val totalAllocated = allocations.foldLeft(Resource.empty){ (totalResource, resourceAllocation) =>
      totalResource.add(resourceAllocation.resource)
    }
    LOG.info(s"AppMaster $appId received resource $totalAllocated, ${taskSet.size} tasks remain to be launched")
    totalAllocated.slots == taskSet.size
  }

  def appManagerMsgHandler: Receive = {
    case appMasterDataDetailRequest: AppMasterDataDetailRequest =>
      val appId = appMasterDataDetailRequest.appId
      LOG.info(s"Received AppMasterDataDetailRequest $appId")
      appId match {
        case this.appId =>
          LOG.info(s"Sending back AppMasterDataDetailRequest $appId")
          sender ! AppMasterDataDetail(appId = appId, appDescription = appDescription)
      }
    case ReplayFromTimestampWindowTrailingEdge =>
      if(!restarting){
        (clockService ? GetLatestMinClock).asInstanceOf[Future[LatestMinClock]].map { clock =>
          startClock = clock.clock
          taskLocations = taskLocations.empty
          startedTasks = startedTasks.empty

          if(taskSet.hasNotLaunchedTask){
            val resourceRequests = taskSet.fetchResourceRequests(true)
            resourceRequests.foreach(master ! RequestResource(appId, _))
          }
          context.children.foreach(_ ! RestartTasks(startClock))
        }
        this.restarting = true
      }
  }

  def executorMsgHandler: Receive = {
    case RegisterTask(taskId, executorId, host) =>
      LOG.info(s"Task $taskId has been Launched for app $appId")

      var taskIds = taskLocations.getOrElse(host, Set.empty[TaskId])
      taskIds += taskId
      taskLocations += host -> taskIds

      var taskSetForExecutorId = executorIdToTasks.getOrElse(executorId, Set.empty[TaskId])
      taskSetForExecutorId += taskId
      executorIdToTasks += executorId -> taskSetForExecutorId

      startedTasks += taskId

      LOG.info(s" started task size: ${startedTasks.size}, taskQueue size: ${taskSet.totalTaskCount}")
      if (startedTasks.size == taskSet.totalTaskCount) {
        this.restarting = false
        context.children.foreach { executor =>
          LOG.info(s"Sending Task locations to executor ${executor.path.name}")
          executor ! TaskLocations(taskLocations)
        }
      }
      sender ! StartClock(startClock)
    case clock : UpdateClock =>
      clockService forward clock
    case GetLatestMinClock =>
      clockService forward GetLatestMinClock

    case task: TaskFinished =>
      val taskId = task.taskId
      LOG.info(s"Task $taskId has been finished for app $appId")

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

  def selfMsgHandler : Receive = {
    case LaunchExecutorActor(conf : Props, executorId : Int, daemon : ActorRef) =>
      val executor = context.actorOf(conf, executorId.toString)
      daemon ! BindLifeCycle(executor)
  }

  def workerMsgHandler : Receive = {
    case RegisterExecutor(executor, executorId, resource, workerId) =>
      LOG.info(s"executor $executorId has been launched")
      //watch for executor termination
      context.watch(executor)

      def launchTask(remainResources: Resource): Unit = {
        if (remainResources.greaterThan(Resource.empty) && taskSet.hasNotLaunchedTask) {
          val TaskLaunchData(taskId, taskDescription, dag) = taskSet.scheduleTaskOnWorker(workerId)
          //Launch task

          LOG.info("Sending Launch Task to executor: " + executor.toString())

          val executorByPath = context.actorSelection("../app_0_executor_0")

          val config = appDescription.conf.withAppId(appId).withUserName(username).withExecutorId(executorId).withAppMaster(self).withDag(dag)
          executor ! LaunchTask(taskId, config, ActorUtil.loadClass(taskDescription.taskClass))
          //Todo: subtract the actual resource used by task
          val usedResource = Resource(1)
          launchTask(remainResources subtract usedResource)
        }
      }
      launchTask(resource)
    case ExecutorLaunchRejected(reason, resource, ex) =>
      LOG.error(s"Executor Launch failed reason：$reason", ex)
      LOG.info(s"reallocate resource $resource to start appmaster")
      master ! RequestResource(appId, ResourceRequest(resource))
  }

  //TODO: We an task is down, we need to recover
  def terminationWatch : Receive = {
    case Terminated(actor) =>
    if (null != master && actor.compareTo(master) == 0) {
      // master is down, let's try to contact new master
      LOG.info("parent master cannot be contacted, find a new master ...")
      context.become(waitForMasterToConfirmRegistration(repeatActionUtil(30)(masterProxy ! RegisterAppMaster(self, appId, masterExecutorId, resource, registerData))))
    } else if (ActorUtil.isChildActorPath(self, actor)) {
      //executor is down
      //TODO: handle this failure

      val executorId = actor.path.name.toInt
      LOG.error(s"Executor is down ${actor.path.name}, executorId: $executorId")

      val tasks = executorIdToTasks(executorId)
      taskSet.taskFailed(tasks)

      self ! ReplayFromTimestampWindowTrailingEdge
    } else {
      LOG.error(s"=============terminiated unknown actorss===============${actor.path}")
    }
  }

  import context.dispatcher

  private def repeatActionUtil(seconds: Int)(action : => Unit) : Cancellable = {

    val cancelSend = context.system.scheduler.schedule(Duration.Zero, Duration(2, TimeUnit.SECONDS))(action)
    val cancelSuicide = context.system.scheduler.scheduleOnce(FiniteDuration(seconds, TimeUnit.SECONDS), self, PoisonPill)
    new Cancellable {
      def cancel(): Boolean = {
        val result1 = cancelSend.cancel()
        val result2 = cancelSuicide.cancel()
        result1 && result2
      }

      def isCancelled: Boolean = {
        cancelSend.isCancelled && cancelSuicide.isCancelled
      }
    }
  }

  private def snapshotStartClock() : Unit = {
    (clockService ? GetLatestMinClock).asInstanceOf[Future[LatestMinClock]].map { clock =>
      store.put(START_CLOCK, clock.clock)
    }
  }

  override def postStop() : Unit = {
    updateScheduler.cancel()
  }
}

object AppMaster {

  case class TaskLaunchData(taskId: TaskId, taskDescription : TaskDescription, dag : DAG)

  object LaunchActorSystemTimeOut
  case object AllocateResourceTimeOut

  class ExecutorLauncher (worker : ActorRef, appId : Int, executorId : Int, resource : Resource, executorConfig : Configs, jar: Option[AppJar]) extends Actor {

    val username = executorConfig.username

    private val LOG: Logger = LogUtil.getLogger(getClass, app = appId, executor = executorId)

    val name = ActorUtil.actorNameForExecutor(appId, executorId)
    val selfPath = ActorUtil.getFullPath(context.system, self.path)
    val extraClasspath = context.system.settings.config.getString(Constants.GEARPUMP_EXECUTOR_EXTRA_CLASSPATH)
    val classPath = Array.concat(extraClasspath.split(File.pathSeparator))
    val launch = ExecutorContext(classPath, executorConfig.getString(Constants.GEARPUMP_EXECUTOR_ARGS).split(" "), classOf[ActorSystemBooter].getName, Array(name, selfPath), jar, username)
    worker ! LaunchExecutor(appId, executorId, resource, launch)

    implicit val executionContext = context.dispatcher
    val timeout = context.system.scheduler.scheduleOnce(Duration(15, TimeUnit.SECONDS), self, LaunchActorSystemTimeOut)

    def receive : Receive = waitForActorSystemToStart

    def waitForActorSystemToStart : Receive = {
      case RegisterActorSystem(systemPath) =>
        timeout.cancel()
        LOG.info(s"Received RegisterActorSystem $systemPath for app master")
        val executorProps = Props(classOf[Executor], executorConfig).withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(systemPath))))
        sender ! BindLifeCycle(worker)
        context.parent ! LaunchExecutorActor(executorProps, executorConfig.executorId, sender())
        context.stop(self)
      case LaunchActorSystemTimeOut =>
        LOG.error("The Executor ActorSystem has not been started in time, cannot start Executor" +
          "in it...")
        context.stop(self)
      case rejected: ExecutorLaunchRejected =>
        context.parent ! rejected
        context.stop(self)
    }
  }

  case class LaunchExecutorActor(executorConfig : Props, executorId : Int, daemon: ActorRef)
}
