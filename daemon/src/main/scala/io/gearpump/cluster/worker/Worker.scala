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

package io.gearpump.cluster.worker

import java.io.File
import java.lang.management.ManagementFactory
import java.net.URL
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import akka.actor._
import akka.pattern.pipe
import com.typesafe.config.{Config, ConfigFactory}
import io.gearpump.cluster.master.Master.MasterInfo
import io.gearpump.metrics.Metrics.ReportMetrics
import io.gearpump.util.{Constants, TimeOutScheduler}
import io.gearpump.cluster.AppMasterToMaster.{WorkerData, GetWorkerData}
import io.gearpump.cluster.AppMasterToWorker._
import io.gearpump.cluster.ClientToMaster.{QueryHistoryMetrics, QueryWorkerConfig}
import io.gearpump.cluster.{ExecutorContext, ExecutorJVMConfig, ClusterConfig}
import io.gearpump.cluster.MasterToClient.WorkerConfig
import io.gearpump.cluster.MasterToWorker._
import io.gearpump.cluster.WorkerToAppMaster._
import io.gearpump.cluster.WorkerToMaster._
import io.gearpump.cluster.master.Master.MasterInfo
import io.gearpump.cluster.scheduler.Resource
import io.gearpump.cluster.worker.Worker._
import io.gearpump.metrics.Metrics.ReportMetrics
import io.gearpump.metrics.{MetricsReporterService, Metrics, JvmMetricsSet}
import io.gearpump.util.Constants._
import io.gearpump.util.HistoryMetricsService.HistoryMetricsConfig
import io.gearpump.jarstore.{ActorSystemRequired, ConfigRequired, JarStoreService}
import io.gearpump.util._
import org.slf4j.Logger

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * masterProxy is used to resolve the master
 */
private[cluster] class Worker(masterProxy : ActorRef) extends Actor with TimeOutScheduler{
  import Worker._

  private val systemConfig : Config = context.system.settings.config
  private val configStr = systemConfig.root().render

  private val address = ActorUtil.getFullPath(context.system, self.path)
  private var resource = Resource.empty
  private var allocatedResources = Map[ActorRef, Resource]()
  private var executorsInfo = Map[ActorRef, ExecutorSlots]()
  private var id = -1
  private val createdTime = System.currentTimeMillis()
  private var masterInfo: MasterInfo = null
  private var executorNameToActor = Map.empty[String, ActorRef]
  private val jarStoreService = JarStoreService.get(systemConfig)
  jarStoreService match {
    case needConfig: ConfigRequired => needConfig.init(systemConfig)
    case needSystem: ActorSystemRequired => needSystem.init(context.system)
  }

  private val ioPool = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

  private var totalSlots: Int = 0

  val metricsEnabled = systemConfig.getBoolean(GEARPUMP_METRIC_ENABLED)
  var historyMetricsService: Option[ActorRef] = None

  override def receive : Receive = null
  var LOG: Logger = LogUtil.getLogger(getClass)

  def service: Receive =
    appMasterMsgHandler orElse
      clientMessageHandler orElse
      metricsService orElse
      terminationWatch(masterInfo.master) orElse
      ActorUtil.defaultMsgHandler(self)

  def metricsService : Receive = {
    case query: QueryHistoryMetrics =>
      historyMetricsService.foreach(_ forward query)
  }

  def waitForMasterConfirm(killSelf : Cancellable) : Receive = {
    case WorkerRegistered(id, masterInfo) =>
      this.id = id
      // register jvm metrics
      Metrics(context.system).register(new JvmMetricsSet(s"worker${id}"))

      historyMetricsService = if (metricsEnabled) {
        val getHistoryMetricsConfig = HistoryMetricsConfig(systemConfig)
        val historyMetricsService = {
          context.actorOf(Props(new HistoryMetricsService("worker" + id, getHistoryMetricsConfig)))
        }

        val metricsReportService = context.actorOf(Props(new MetricsReporterService(Metrics(context.system))))
        historyMetricsService.tell(ReportMetrics, metricsReportService)
        Some(historyMetricsService)
      } else {
        None
      }

      this.masterInfo = masterInfo
      killSelf.cancel()
      context.watch(masterInfo.master)
      this.LOG = LogUtil.getLogger(getClass, worker = id)
      LOG.info(s"Worker is registered. actor path: ${ActorUtil.getFullPath(context.system, self.path)} ....")
      sendMsgWithTimeOutCallBack(masterInfo.master, ResourceUpdate(self, id, resource), 30, updateResourceTimeOut())
      context.become(service)
  }

  private def updateResourceTimeOut(): Unit = {
    LOG.error(s"Update worker resource time out")
  }

  def appMasterMsgHandler : Receive = {
    case shutdown @ ShutdownExecutor(appId, executorId, reason : String) =>
      val actorName = ActorUtil.actorNameForExecutor(appId, executorId)
      val executorToStop = executorNameToActor.get(actorName)
      if (executorToStop.isDefined) {
        LOG.info(s"Shutdown executor ${actorName}(${executorToStop.get.path.toString}) due to: $reason")
        executorToStop.get.forward(shutdown)
      } else {
        LOG.error(s"Cannot find executor $actorName, ignore this message")
        sender ! ShutdownExecutorFailed(s"Can not find executor $executorId for app $appId")
      }
    case launch : LaunchExecutor =>
      LOG.info(s"$launch")
      if (resource < launch.resource) {
        sender ! ExecutorLaunchRejected("There is no free resource on this machine")
      } else {
        val actorName = ActorUtil.actorNameForExecutor(launch.appId, launch.executorId)

        val executor = context.actorOf(Props(classOf[ExecutorWatcher], launch, masterInfo, ioPool, jarStoreService))
        executorNameToActor += actorName ->executor

        resource = resource - launch.resource
        allocatedResources = allocatedResources + (executor -> launch.resource)

        reportResourceToMaster
        executorsInfo += executor ->
          ExecutorSlots(launch.appId, launch.executorId, launch.resource.slots)
        context.watch(executor)
      }
    case UpdateResourceFailed(reason, ex) =>
      LOG.error(reason)
      context.stop(self)
    case UpdateResourceSucceed =>
      LOG.info(s"Update resource succeed")
    case GetWorkerData(workerId) =>
      val aliveFor = System.currentTimeMillis() - createdTime
      val logDir = LogUtil.daemonLogDir(systemConfig).getAbsolutePath
      val userDir = System.getProperty("user.dir")
      sender ! WorkerData(WorkerSummary(
        id, "active",
        address,
        aliveFor,
        logDir,
        executorsInfo.values.toArray,
        totalSlots,
        resource.slots,
        userDir,
        jvmName = ManagementFactory.getRuntimeMXBean().getName()))
    case ChangeExecutorResource(appId, executorId, usedResource) =>
      for (executor <- executorActorRef(appId, executorId);
           allocatedResource <- allocatedResources.get(executor)) {
        allocatedResources += executor -> usedResource
        resource = resource + allocatedResource - usedResource
        reportResourceToMaster

        if (usedResource == Resource(0)) {
          allocatedResources -= executor
          // stop executor if there is no resource binded to it.
          executor ! ShutdownExecutor(appId, executorId, "Shutdown executor because the resource used is zero")
        }
      }
  }

  private def reportResourceToMaster: Unit = {
    sendMsgWithTimeOutCallBack(masterInfo.master,
      ResourceUpdate(self, id, resource), 30, updateResourceTimeOut())
  }

  private def executorActorRef(appId: Int, executorId: Int): Option[ActorRef] = {
    val actorName = ActorUtil.actorNameForExecutor(appId, executorId)
    executorNameToActor.get(actorName)
  }

  def clientMessageHandler: Receive = {
    case QueryWorkerConfig(workerId) =>
      if (this.id == workerId) {
        sender ! WorkerConfig(systemConfig)
      } else {
        sender ! WorkerConfig(ConfigFactory.empty)
      }
  }

  def terminationWatch(master : ActorRef) : Receive = {
    case Terminated(actor) =>
      if (actor.compareTo(master) == 0) {
        // parent is down, let's make suicide
        LOG.info(s"Master cannot be contacted, find a new master ...")
        context.become(waitForMasterConfirm(repeatActionUtil(30)(masterProxy ! RegisterWorker(id))))
      } else if (ActorUtil.isChildActorPath(self, actor)) {
        //one executor is down,
        LOG.info(s"Executor is down ${getExecutorName(actor)}")

        val allocated = allocatedResources.get(actor)
        if (allocated.isDefined) {
          resource = resource + allocated.get
          executorsInfo -= actor
          allocatedResources = allocatedResources - actor
          sendMsgWithTimeOutCallBack(master, ResourceUpdate(self, id, resource), 30, updateResourceTimeOut())
        }
      }
  }

  private def getExecutorName(actorRef: ActorRef):  Option[String] = {
    executorNameToActor.find(_._2 == actorRef).map(_._1)
  }

  import context.dispatcher
  override def preStart() : Unit = {
    LOG.info(s"RegisterNewWorker")
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
    LOG.info(s"Worker is going down....")
    ioPool.shutdown()
    context.system.shutdown()
  }
}

private[cluster] object Worker {

  case class ExecutorResult(result : Try[Int])

  class ExecutorWatcher(launch: LaunchExecutor, masterInfo: MasterInfo, ioPool: ExecutionContext, jarStoreService: JarStoreService) extends Actor {

    val config = context.system.settings.config
    private val LOG: Logger = LogUtil.getLogger(getClass, app = launch.appId, executor = launch.executorId)

    implicit val executorService = ioPool

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
        createProcess(ctx)
      }
    }

    private def createProcess(ctx: ExecutorJVMConfig): ExecutorHandler = {

      val process = Future {
        val jarPath = ctx.jar.map { appJar =>
          val tempFile = File.createTempFile(appJar.name, ".jar")
          jarStoreService.copyToLocalFile(tempFile, appJar.filePath)
          val file = new URL("file:" + tempFile)
          file.getFile
        }

        val configFile = Option(ctx.executorAkkaConfig).filterNot(_.isEmpty).map { conf =>
          val configFile = File.createTempFile("gearpump", ".conf")
          ClusterConfig.saveConfig(conf, configFile)
          val file = new URL("file:" + configFile)
          file.getFile
        }

        val classPath = filterDaemonLib(Util.getCurrentClassPath) ++
          ctx.classPath.map(path => expandEnviroment(path)) ++
          jarPath.map(Array(_)).getOrElse(Array.empty[String])


        val appLogDir = context.system.settings.config.getString(Constants.GEARPUMP_LOG_APPLICATION_DIR)
        val logArgs = List(
          s"-D${Constants.GEARPUMP_APPLICATION_ID}=${launch.appId}",
          s"-D${Constants.GEARPUMP_EXECUTOR_ID}=${launch.executorId}",
          s"-D${Constants.GEARPUMP_MASTER_STARTTIME}=${getFormatedTime(masterInfo.startTime)}",
          s"-D${Constants.GEARPUMP_LOG_APPLICATION_DIR}=${appLogDir}")
        val configArgs = configFile.map(confFilePath =>
          List(s"-D${Constants.GEARPUMP_CUSTOM_CONFIG_FILE}=$confFilePath")
        ).getOrElse(List.empty[String])

        // pass hostname as a JVM parameter, so that child actorsystem can read it
        // in priority
        val host = Try(context.system.settings.config.getString(Constants.GEARPUMP_HOSTNAME)).map(
          host => List(s"-D${Constants.GEARPUMP_HOSTNAME}=${host}")).getOrElse(List.empty[String])

        val username = List(s"-D${Constants.GEARPUMP_USERNAME}=${ctx.username}")

        //remote debug executor process
        val remoteDebugFlag = config.getBoolean(Constants.GEARPUMP_REMOTE_DEBUG_EXECUTOR_JVM)
        val remoteDebugConfig = if (remoteDebugFlag) {
          val availablePort = Util.findFreePort.get
          List(
            "-Xdebug",
            s"-Xrunjdwp:server=y,transport=dt_socket,address=${availablePort},suspend=n",
            s"-D${Constants.GEARPUMP_REMOTE_DEBUG_PORT}=$availablePort"
          )
        } else {
          List.empty[String]
        }

        val verboseGCFlag = config.getBoolean(Constants.GEARPUMP_VERBOSE_GC)
        val verboseGCConfig = if (verboseGCFlag) {
          List(
            s"-Xloggc:${appLogDir}/gc-app${launch.appId}-executor-${launch.executorId}.log",
            "-verbose:gc",
            "-XX:+PrintGCDetails",
            "-XX:+PrintGCDateStamps",
            "-XX:+PrintTenuringDistribution",
            "-XX:+PrintGCApplicationConcurrentTime",
            "-XX:+PrintGCApplicationStoppedTime"
          )
        } else {
          List.empty[String]
        }

        val options = ctx.jvmArguments ++ host ++ username ++
          logArgs ++ remoteDebugConfig ++ verboseGCConfig ++ configArgs

        LOG.info(s"Launch executor, classpath: ${classPath.mkString(File.pathSeparator)}")
        val process = Util.startProcess(options, classPath, ctx.mainClass, ctx.arguments)

        ProcessInfo(process, jarPath, configFile)
      }

      new ExecutorHandler {

        var destroyed = false

        override def destroy: Unit = {
          LOG.info(s"Destroy executor process ${ctx.mainClass}")
          if (!destroyed) {
            destroyed = true
            process.foreach { info =>
              info.process.destroy()
              info.jarPath.foreach(new File(_).delete())
              info.configFile.foreach(new File(_).delete())
            }
          }
        }

        override def exitValue: Future[Try[Int]] = {
          process.map { info =>
            val exit = info.process.exitValue()
            if (exit == 0) {
              Success(0)
            } else {
              Failure(new Exception(s"Executor exit with failure, exit value: $exit, error summary: ${info.process.logger.summary}"))
            }
          }
        }
      }
    }

    import Constants._
    private def expandEnviroment(path: String): String = {
      //TODO: extend this to support more environment.
      path.replace(s"<${GEARPUMP_HOME}>", config.getString(GEARPUMP_HOME))
    }

    override def preStart: Unit = {
      executorHandler.exitValue.map(ExecutorResult).pipeTo(self)
    }

    override def postStop: Unit = {
      executorHandler.destroy
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
      val datePattern = "yyyy-MM-dd-HH-mm"
      val format = new java.text.SimpleDateFormat(datePattern)
      format.format(timestamp)
    }
  }

  private def filterDaemonLib(classPath: Array[String]): Array[String] = {
    val jarFile = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath)
    val daemonDir = jarFile.getParent
    classPath.filterNot{path =>
      //The class path maybe a symbol link
      val canonicalized = Try(new File(path).getCanonicalPath)
      if(canonicalized.isSuccess && canonicalized.get.startsWith(daemonDir)) {
        true
      } else {
        false
      }
    }
  }

  trait ExecutorHandler {
    def destroy : Unit
    def exitValue : Future[Try[Int]]
  }

  case class ProcessInfo(process: RichProcess, jarPath: Option[String], configFile: Option[String])
}