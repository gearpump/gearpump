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

package org.apache.gearpump.cluster.worker

import java.io.File
import java.net.URL
import java.util.concurrent.{Executors, TimeUnit}

import akka.actor._
import akka.pattern.pipe
import com.typesafe.config.Config
import org.apache.gearpump.cluster.AppMasterToMaster.{WorkerData, GetWorkerData}
import org.apache.gearpump.cluster.AppMasterToWorker._
import org.apache.gearpump.cluster.ClusterConfig
import org.apache.gearpump.cluster.MasterToWorker._
import org.apache.gearpump.cluster.WorkerToAppMaster._
import org.apache.gearpump.cluster.WorkerToMaster._
import org.apache.gearpump.cluster.master.Master.MasterInfo
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.cluster.worker.Worker.ExecutorInfo
import org.apache.gearpump.util._
import org.slf4j.Logger

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class WorkerDescription(workerId: Int, state: String, actorPath: String,
                             aliveFor: Long, logFile: String,
                             executors: Array[ExecutorInfo], totalSlots: Int, availableSlots: Int,
                             homeDirectory: String)
/**
 * masterProxy is used to resolve the master
 */
private[cluster] class Worker(masterProxy : ActorRef) extends Actor with TimeOutScheduler{
  import Worker._

  private val systemConfig : Config = context.system.settings.config
  private val configStr = systemConfig.root().render

  private val address = ActorUtil.getFullPath(context.system, self.path)
  private var resource = Resource.empty
  private var allocatedResource = Map[ActorRef, Resource]()
  private var executorsInfo = Map[ActorRef, ExecutorInfo]()
  private var id = -1
  private val createdTime = System.currentTimeMillis()
  private var masterInfo: MasterInfo = null
  private var executorNameToActor = Map.empty[String, ActorRef]

  private var totalSlots: Int = 0

  override def receive : Receive = null
  val LOG : Logger = LogUtil.getLogger(getClass, worker = id)

  def waitForMasterConfirm(killSelf : Cancellable) : Receive = {
    case WorkerRegistered(id, masterInfo) =>
      this.id = id
      this.masterInfo = masterInfo
      killSelf.cancel()
      context.watch(masterInfo.master)
      LOG.info(s"Worker $id Registered ....")
      sendMsgWithTimeOutCallBack(masterInfo.master, ResourceUpdate(self, id, resource), 30, updateResourceTimeOut())
      context.become(appMasterMsgHandler orElse terminationWatch(masterInfo.master) orElse ActorUtil.defaultMsgHandler(self))
  }

  private def updateResourceTimeOut(): Unit = {
    LOG.error(s"Worker $id update resource time out")
  }

  def appMasterMsgHandler : Receive = {
    case shutdown @ ShutdownExecutor(appId, executorId, reason : String) =>
      val actorName = ActorUtil.actorNameForExecutor(appId, executorId)
      LOG.info(s"Worker shutting down executor: $actorName due to: $reason")
      val executorToStop = executorNameToActor.get(actorName)

      if (executorToStop.isDefined) {
        LOG.info(s"Shuttting down child: ${executorToStop.get.path.toString}")
        executorToStop.get.forward(shutdown)
      } else {
        LOG.info(s"There is no child $actorName, ignore this message")
        sender ! ShutdownExecutorFailed(s"Can not find executor $executorId for app $appId")
      }
    case launch : LaunchExecutor =>
      LOG.info(s"Worker[$id] LaunchExecutor ....$launch")
      if (resource < launch.resource) {
        sender ! ExecutorLaunchRejected("There is no free resource on this machine")
      } else {
        val actorName = ActorUtil.actorNameForExecutor(launch.appId, launch.executorId)

        val executor = context.actorOf(Props(classOf[ExecutorWatcher], launch, masterInfo))
        executorNameToActor += actorName ->executor

        resource = resource - launch.resource
        allocatedResource = allocatedResource + (executor -> launch.resource)

        sendMsgWithTimeOutCallBack(masterInfo.master,
          ResourceUpdate(self, id, resource), 30, updateResourceTimeOut())
        executorsInfo += executor -> ExecutorInfo(launch.appId, launch.executorId, launch.resource.slots)
        context.watch(executor)
      }
    case UpdateResourceFailed(reason, ex) =>
      LOG.error(reason)
      context.stop(self)
    case UpdateResourceSucceed =>
      LOG.info(s"Worker $id update resource succeed")
    case GetWorkerData(workerId) =>
      val aliveFor = System.currentTimeMillis() - createdTime
      val logDir = LogUtil.daemonLogDir(systemConfig).getAbsolutePath
      val userDir = System.getProperty("user.dir");
      sender ! WorkerData(Some(WorkerDescription(id, "active", address,
        aliveFor, logDir, executorsInfo.values.toArray, totalSlots, resource.slots, userDir)))
  }

  def terminationWatch(master : ActorRef) : Receive = {
    case Terminated(actor) =>
      if (actor.compareTo(master) == 0) {
        // parent is down, let's make suicide
        LOG.info("parent master cannot be contacted, find a new master ...")
        context.become(waitForMasterConfirm(repeatActionUtil(30)(masterProxy ! RegisterWorker(id))))
      } else if (ActorUtil.isChildActorPath(self, actor)) {
        //one executor is down,
        LOG.info(s"Executor is down ${actor.path.name}")

        val allocated = allocatedResource.get(actor)
        if (allocated.isDefined) {
          resource = resource + allocated.get
          executorsInfo -= actor
          allocatedResource = allocatedResource - actor
          sendMsgWithTimeOutCallBack(master, ResourceUpdate(self, id, resource), 30, updateResourceTimeOut())
        }
      }
  }

  import context.dispatcher
  override def preStart() : Unit = {
    LOG.info(s"Worker[$id] Sending master RegisterNewWorker")
    totalSlots = systemConfig.getInt(Constants.GEARPUMP_WORKER_SLOTS)
    this.resource = Resource(totalSlots)
    masterProxy ! RegisterNewWorker
    context.become(waitForMasterConfirm(repeatActionUtil(30)(Unit)))
  }

  private def repeatActionUtil(seconds: Int)(action : => Unit) : Cancellable = {

    val cancelSend = context.system.scheduler.schedule(Duration.Zero, Duration(2, TimeUnit.SECONDS))(action)
    val cancelSuicide = context.system.scheduler.scheduleOnce(FiniteDuration(seconds, TimeUnit.SECONDS), self, PoisonPill)
    return new Cancellable {
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

  override def postStop : Unit = {
    LOG.info(s"Worker $id is going down....")
    context.system.shutdown()
  }
}

private[cluster] object Worker {

  case class ExecutorResult(result : Try[Int])
  case class ExecutorInfo(appId: Int, executorId: Int, slots: Int)


  class ExecutorWatcher(launch: LaunchExecutor, masterInfo: MasterInfo) extends Actor {

    implicit val executionContext = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

    private val LOG: Logger = LogUtil.getLogger(getClass, app = launch.appId, executor = launch.executorId)

    private val executorHandler = {
      val ctx = launch.executorJvmConfig
      if (System.getProperty("LOCAL") != null) {
        new ExecutorHandler {
          override def destroy = Unit // we cannot forcefully terminate a future by scala limit
          override def exitValue : Future[Try[Int]] = Future {
              try {
                val clazz = Class.forName(ctx.mainClass)
                val main = clazz.getMethod("main", classOf[Array[String]])
                main.invoke(null, ctx.arguments)
                Success(0)
              } catch {
                case e: Throwable => Failure(e)
              }
            }
        }
      } else {
        val appJar = ctx.jar

        val jarPath = appJar.map {appJar =>
          val tempFile = File.createTempFile(appJar.name, ".jar")
          appJar.container.copyToLocalFile(tempFile)
          val file = new URL("file:"+tempFile)
          file.getFile
        }

        val configFile = Option(ctx.executorAkkaConfig).filterNot(_.isEmpty).map{conf =>
          val configFile = File.createTempFile("gearpump", ".conf")
          ClusterConfig.saveConfig(conf, configFile)
          val file = new URL("file:" + configFile)
          file.getFile
        }

        val classPathPrefix = Util.getCurrentClassPath ++ ctx.classPath
        val classPath = jarPath.map(classPathPrefix :+ _).getOrElse(classPathPrefix)

        val logArgs = List(
          s"-D${Constants.GEARPUMP_APPLICATION_ID}=${launch.appId}",
          s"-D${Constants.GEARPUMP_EXECUTOR_ID}=${launch.executorId}",
          s"-D${Constants.GEARPUMP_MASTER_STARTTIME}=${getFormatedTime(masterInfo.startTime)}")
        val configArgs = configFile.map(confFilePath =>
          List(s"-D${Constants.GEARPUMP_CUSTOM_CONFIG_FILE}=$confFilePath")
          ).getOrElse(List.empty[String])

        // pass hostname as a JVM parameter, so that child actorsystem can read it
        // in priority
        val host = Try(context.system.settings.config.getString(Constants.NETTY_TCP_HOSTNAME)).map(
          host => List(s"-D${Constants.NETTY_TCP_HOSTNAME}=${host}")).getOrElse(List.empty[String])

        val username = List(s"-D${Constants.GEARPUMP_USERNAME}=${ctx.username}")

        val options = ctx.jvmArguments ++ host ++ username ++ logArgs ++ configArgs
        val process = Util.startProcess(options, classPath, ctx.mainClass, ctx.arguments)

        new ExecutorHandler {
          override def destroy = {
            LOG.info(s"destroying executor process ${ctx.mainClass}")
            process.destroy()
            deleteTempFile
          }

          override def exitValue: Future[Try[Int]] = Future {
            val exit = process.exitValue()
            if (exit == 0) {
              Success(0)
            } else {
              Failure(new Exception(s"Executor exit with error, exit value: $exit"))
            }
          }

          def deleteTempFile : Unit = {
            jarPath.map(new File(_)).map(_.delete())
            configFile.map(new File(_)).map(_.delete())
          }
        }
      }
    }

    override def preStart: Unit = {
      executorHandler.exitValue.map(ExecutorResult).pipeTo(self)
    }

    override def receive: Receive = {
      case ShutdownExecutor(appId, executorId, reason : String) =>
        executorHandler.destroy
        sender ! ShutdownExecutorSucceed(appId, executorId)
        context.stop(self)
      case ExecutorResult(executorResult) =>
        executorResult match {
          case Success(exit) => LOG.info("Executor exit normally with exit value " + exit)
          case Failure(e) => LOG.error("Executor exit with errors", e)
        }
        context.stop(self)
    }

    private def getFormatedTime(timestamp: Long): String = {
      val datePattern = "yyyy-MM-dd-hh-mm"
      val format = new java.text.SimpleDateFormat(datePattern)
      format.format(timestamp)
    }
  }

  trait ExecutorHandler {
    def destroy : Unit
    def exitValue : Future[Try[Int]]
  }
}
