/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.cluster.worker

import java.io.File
import java.lang.management.ManagementFactory
import java.net.URL
import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.gearpump.cluster.AppMasterToMaster.{GetWorkerData, WorkerData}
import org.apache.gearpump.cluster.AppMasterToWorker._
import org.apache.gearpump.cluster.ClientToMaster.{QueryHistoryMetrics, QueryWorkerConfig}
import org.apache.gearpump.cluster.MasterToClient.{HistoryMetrics, HistoryMetricsItem, WorkerConfig}
import org.apache.gearpump.cluster.MasterToWorker.{UpdateResourceSucceed, UpdateResourceFailed, WorkerRegistered}
import org.apache.gearpump.cluster.WorkerToAppMaster._
import org.apache.gearpump.cluster.WorkerToMaster.{RegisterNewWorker, RegisterWorker, ResourceUpdate}
import org.apache.gearpump.cluster.master.Master.MasterInfo
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.cluster.worker.Worker.ExecutorWatcher
import org.apache.gearpump.cluster.{ClusterConfig, ExecutorJVMConfig}
import org.apache.gearpump.jarstore.JarStoreClient
import org.apache.gearpump.metrics.Metrics.ReportMetrics
import org.apache.gearpump.metrics.{JvmMetricsSet, Metrics, MetricsReporterService}
import org.apache.gearpump.util.ActorSystemBooter.Daemon
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.HistoryMetricsService.HistoryMetricsConfig
import org.apache.gearpump.util.{TimeOutScheduler, _}
import org.slf4j.Logger

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * Worker is used to track the resource on single machine, it is like
 * the node manager of YARN.
 *
 * @param masterProxy masterProxy is used to resolve the master
 */
private[cluster] class Worker(masterProxy: ActorRef) extends Actor with TimeOutScheduler {
  private val systemConfig: Config = context.system.settings.config

  private val address = ActorUtil.getFullPath(context.system, self.path)
  private var resource = Resource.empty
  private var allocatedResources = Map[ActorRef, Resource]()
  private var executorsInfo = Map[ActorRef, ExecutorSlots]()
  private var id: WorkerId = WorkerId.unspecified
  private val createdTime = System.currentTimeMillis()
  private var masterInfo: MasterInfo = null
  private var executorNameToActor = Map.empty[String, ActorRef]
  private val executorProcLauncher: ExecutorProcessLauncher = getExecutorProcLauncher()
  private val jarStoreClient = new JarStoreClient(systemConfig, context.system)

  private val ioPool = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
  private val resourceUpdateTimeoutMs = 30000 // Milliseconds

  private var totalSlots: Int = 0

  val metricsEnabled = systemConfig.getBoolean(GEARPUMP_METRIC_ENABLED)
  var historyMetricsService: Option[ActorRef] = None

  override def receive: Receive = null
  var LOG: Logger = LogUtil.getLogger(getClass)

  def service: Receive =
    appMasterMsgHandler orElse
      clientMessageHandler orElse
      metricsService orElse
      terminationWatch(masterInfo.master) orElse
      ActorUtil.defaultMsgHandler(self)

  def metricsService: Receive = {
    case query: QueryHistoryMetrics =>
      if (historyMetricsService.isEmpty) {
        // Returns empty metrics so that we don't hang the UI
        sender ! HistoryMetrics(query.path, List.empty[HistoryMetricsItem])
      } else {
        historyMetricsService.get forward query
      }
  }

  private var metricsInitialized = false

  val getHistoryMetricsConfig = HistoryMetricsConfig(systemConfig)

  private def initializeMetrics(): Unit = {
    // Registers jvm metrics
    val metricsSetName = "worker" + WorkerId.render(id)
    Metrics(context.system).register(new JvmMetricsSet(metricsSetName))

    historyMetricsService = if (metricsEnabled) {
      val historyMetricsService = {
        context.actorOf(Props(new HistoryMetricsService(metricsSetName, getHistoryMetricsConfig)))
      }

      val metricsReportService = context.actorOf(Props(
        new MetricsReporterService(Metrics(context.system))))
      historyMetricsService.tell(ReportMetrics, metricsReportService)
      Some(historyMetricsService)
    } else {
      None
    }
  }

  def waitForMasterConfirm(timeoutTicker: Cancellable): Receive = {

    // If master get disconnected, the WorkerRegistered may be triggered multiple times.
    case WorkerRegistered(id, masterInfo) =>
      this.id = id

      // Adds the flag check, so that we don't re-initialize the metrics when worker re-register
      // itself.
      if (!metricsInitialized) {
        initializeMetrics()
        metricsInitialized = true
      }

      this.masterInfo = masterInfo
      timeoutTicker.cancel()
      context.watch(masterInfo.master)
      this.LOG = LogUtil.getLogger(getClass, worker = id)
      LOG.info(s"Worker is registered. " +
        s"actor path: ${ActorUtil.getFullPath(context.system, self.path)} ....")
      sendMsgWithTimeOutCallBack(masterInfo.master, ResourceUpdate(self, id, resource),
        resourceUpdateTimeoutMs, updateResourceTimeOut())
      context.become(service)
  }

  private def updateResourceTimeOut(): Unit = {
    LOG.error(s"Update worker resource time out")
  }

  def appMasterMsgHandler: Receive = {
    case shutdown@ShutdownExecutor(appId, executorId, reason: String) =>
      val actorName = ActorUtil.actorNameForExecutor(appId, executorId)
      val executorToStop = executorNameToActor.get(actorName)
      if (executorToStop.isDefined) {
        LOG.info(s"Shutdown executor ${actorName}(${executorToStop.get.path.toString}) " +
          s"due to: $reason")
        executorToStop.get.forward(shutdown)
      } else {
        LOG.error(s"Cannot find executor $actorName, ignore this message")
        sender ! ShutdownExecutorFailed(s"Can not find executor $executorId for app $appId")
      }
    case launch: LaunchExecutor =>
      LOG.info(s"$launch")
      if (resource < launch.resource) {
        sender ! ExecutorLaunchRejected("There is no free resource on this machine")
      } else {
        val actorName = ActorUtil.actorNameForExecutor(launch.appId, launch.executorId)

        val executor = context.actorOf(Props(classOf[ExecutorWatcher], launch, masterInfo, ioPool,
          jarStoreClient, executorProcLauncher))
        executorNameToActor += actorName -> executor

        resource = resource - launch.resource
        allocatedResources = allocatedResources + (executor -> launch.resource)

        reportResourceToMaster()
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
        jvmName = ManagementFactory.getRuntimeMXBean().getName(),
        resourceManagerContainerId = systemConfig.getString(
          GEARPUMP_WORKER_RESOURCE_MANAGER_CONTAINER_ID),
        historyMetricsConfig = getHistoryMetricsConfig)
      )
    case ChangeExecutorResource(appId, executorId, usedResource) =>
      for (executor <- executorActorRef(appId, executorId);
        allocatedResource <- allocatedResources.get(executor)) {

        allocatedResources += executor -> usedResource
        resource = resource + allocatedResource - usedResource
        reportResourceToMaster()

        if (usedResource == Resource(0)) {
          executorsInfo -= executor
          allocatedResources -= executor
          // stop executor if there is no resource binded to it.
          LOG.info(s"Shutdown executor $executorId because the resource used is zero")
          executor ! ShutdownExecutor(appId, executorId,
            "Shutdown executor because the resource used is zero")
        }
      }
  }

  private def reportResourceToMaster(): Unit = {
    sendMsgWithTimeOutCallBack(masterInfo.master,
      ResourceUpdate(self, id, resource), resourceUpdateTimeoutMs, updateResourceTimeOut())
  }

  private def executorActorRef(appId: Int, executorId: Int): Option[ActorRef] = {
    val actorName = ActorUtil.actorNameForExecutor(appId, executorId)
    executorNameToActor.get(actorName)
  }

  def clientMessageHandler: Receive = {
    case QueryWorkerConfig(workerId) =>
      if (this.id == workerId) {
        sender ! WorkerConfig(ClusterConfig.filterOutDefaultConfig(systemConfig))
      } else {
        sender ! WorkerConfig(ConfigFactory.empty)
      }
  }

  private def retryRegisterWorker(workerId: WorkerId, timeOutSeconds: Int): Cancellable = {
    repeatActionUtil(
      seconds = timeOutSeconds,
      action = () => {
        masterProxy ! RegisterWorker(workerId)
      },
      onTimeout = () => {
        LOG.error(s"Failed to register the worker $workerId after retrying for $timeOutSeconds " +
          s"seconds, abort and kill the worker...")
        self ! PoisonPill
      })
  }

  def terminationWatch(master: ActorRef): Receive = {
    case Terminated(actor) =>
      if (actor.compareTo(master) == 0) {
        // Parent master is down, no point to keep worker anymore. Let's make suicide to free
        // resources
        LOG.info(s"Master cannot be contacted, find a new master ...")
        context.become(waitForMasterConfirm(retryRegisterWorker(id, timeOutSeconds = 30)))
      } else if (ActorUtil.isChildActorPath(self, actor)) {
        // One executor is down,
        LOG.info(s"Executor is down ${getExecutorName(actor)}")

        val allocated = allocatedResources.get(actor)
        if (allocated.isDefined) {
          resource = resource + allocated.get
          executorsInfo -= actor
          allocatedResources = allocatedResources - actor
          sendMsgWithTimeOutCallBack(master, ResourceUpdate(self, id, resource),
            resourceUpdateTimeoutMs, updateResourceTimeOut())
        }
      }
  }

  private def getExecutorName(actorRef: ActorRef): Option[String] = {
    executorNameToActor.find(_._2 == actorRef).map(_._1)
  }

  private def getExecutorProcLauncher(): ExecutorProcessLauncher = {
    val launcherClazz = Class.forName(
      systemConfig.getString(GEARPUMP_EXECUTOR_PROCESS_LAUNCHER))
    launcherClazz.getConstructor(classOf[Config]).newInstance(systemConfig)
      .asInstanceOf[ExecutorProcessLauncher]
  }

  import context.dispatcher
  override def preStart(): Unit = {
    LOG.info(s"RegisterNewWorker")
    totalSlots = systemConfig.getInt(GEARPUMP_WORKER_SLOTS)
    this.resource = Resource(totalSlots)
    masterProxy ! RegisterNewWorker
    context.become(waitForMasterConfirm(registerTimeoutTicker(seconds = 30)))
  }

  private def registerTimeoutTicker(seconds: Int): Cancellable = {
    repeatActionUtil(seconds, () => Unit, () => {
      LOG.error(s"Failed to register new worker to Master after waiting for $seconds seconds, " +
        s"abort and kill the worker...")
      self ! PoisonPill
    })
  }

  private def repeatActionUtil(seconds: Int, action: () => Unit, onTimeout: () => Unit)
    : Cancellable = {
    val cancelTimeout = context.system.scheduler.schedule(Duration.Zero,
      Duration(2, TimeUnit.SECONDS))(action())
    val cancelSuicide = context.system.scheduler.scheduleOnce(seconds.seconds)(onTimeout())
    new Cancellable {
      def cancel(): Boolean = {
        val result1 = cancelTimeout.cancel()
        val result2 = cancelSuicide.cancel()
        result1 && result2
      }

      def isCancelled: Boolean = {
        cancelTimeout.isCancelled && cancelSuicide.isCancelled
      }
    }
  }

  override def postStop(): Unit = {
    LOG.info(s"Worker is going down....")
    ioPool.shutdown()
    context.system.terminate()
  }
}

private[cluster] object Worker {

  case class ExecutorResult(result: Try[Int])

  class ExecutorWatcher(
      launch: LaunchExecutor,
      masterInfo: MasterInfo,
      ioPool: ExecutionContext,
      jarStoreClient: JarStoreClient,
      procLauncher: ExecutorProcessLauncher) extends Actor {
    import launch.{appId, executorId, resource}

    private val LOG: Logger = LogUtil.getLogger(getClass, app = appId, executor = executorId)

    val executorConfig: Config = {
      val workerConfig = context.system.settings.config

      val submissionConfig = Option(launch.executorJvmConfig).flatMap { jvmConfig =>
        Option(jvmConfig.executorAkkaConfig)
      }.getOrElse(ConfigFactory.empty())

      resolveExecutorConfig(workerConfig, submissionConfig)
    }

    // For some config, worker has priority, for others, user Application submission config
    // have priorities.
    private def resolveExecutorConfig(workerConfig: Config, submissionConfig: Config): Config = {
      val config = submissionConfig.withoutPath(GEARPUMP_HOSTNAME)
        .withoutPath(GEARPUMP_CLUSTER_MASTERS)
        .withoutPath(GEARPUMP_HOME)
        .withoutPath(GEARPUMP_LOG_DAEMON_DIR)
        .withoutPath(GEARPUMP_LOG_APPLICATION_DIR)
        .withoutPath(GEARPUMP_CLUSTER_EXECUTOR_WORKER_SHARE_SAME_PROCESS)
        // Falls back to workerConfig
        .withFallback(workerConfig)

      // Minimum supported akka.scheduler.tick-duration on Windows is 10ms
      val duration = config.getInt(AKKA_SCHEDULER_TICK_DURATION)
      val updatedConf = if (akka.util.Helpers.isWindows && duration < 10) {
        LOG.warn(s"$AKKA_SCHEDULER_TICK_DURATION on Windows must be larger than 10ms, set to 10ms")
        config.withValue(AKKA_SCHEDULER_TICK_DURATION, ConfigValueFactory.fromAnyRef(10))
      } else {
        config
      }

      // Excludes reference.conf, and JVM properties..
      ClusterConfig.filterOutDefaultConfig(updatedConf)
    }

    implicit val executorService = ioPool

    private val executorHandler = {
      val ctx = launch.executorJvmConfig

      if (executorConfig.getBoolean(GEARPUMP_CLUSTER_EXECUTOR_WORKER_SHARE_SAME_PROCESS)) {
        new ExecutorHandler {
          val exitPromise = Promise[Int]()
          val app = context.actorOf(Props(new InJvmExecutor(launch, exitPromise)))

          override def destroy(): Unit = {
            context.stop(app)
          }
          override def exitValue: Future[Int] = {
            exitPromise.future
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
          jarStoreClient.copyToLocalFile(tempFile, appJar.filePath)
          val file = new URL("file:" + tempFile)
          file.getFile
        }

        val configFile = {
          val configFile = File.createTempFile("gearpump", ".conf")
          ClusterConfig.saveConfig(executorConfig, configFile)
          val file = new URL("file:" + configFile)
          file.getFile
        }

        val classPath = filterOutDaemonLib(Util.getCurrentClassPath) ++
          ctx.classPath.map(path => expandEnviroment(path)) ++
          jarPath.map(Array(_)).getOrElse(Array.empty[String])

        val appLogDir = executorConfig.getString(GEARPUMP_LOG_APPLICATION_DIR)
        val logArgs = List(
          s"-D${GEARPUMP_APPLICATION_ID}=${launch.appId}",
          s"-D${GEARPUMP_EXECUTOR_ID}=${launch.executorId}",
          s"-D${GEARPUMP_MASTER_STARTTIME}=${getFormatedTime(masterInfo.startTime)}",
          s"-D${GEARPUMP_LOG_APPLICATION_DIR}=${appLogDir}")
        val configArgs = List(s"-D${GEARPUMP_CUSTOM_CONFIG_FILE}=$configFile")

        val username = List(s"-D${GEARPUMP_USERNAME}=${ctx.username}")

        // Remote debug executor process
        val remoteDebugFlag = executorConfig.getBoolean(GEARPUMP_REMOTE_DEBUG_EXECUTOR_JVM)
        val remoteDebugConfig = if (remoteDebugFlag) {
          val availablePort = Util.findFreePort().get
          List(
            "-Xdebug",
            s"-Xrunjdwp:server=y,transport=dt_socket,address=${availablePort},suspend=n",
            s"-D${GEARPUMP_REMOTE_DEBUG_PORT}=$availablePort"
          )
        } else {
          List.empty[String]
        }

        val verboseGCFlag = executorConfig.getBoolean(GEARPUMP_VERBOSE_GC)
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

        val ipv4 = List(s"-D${PREFER_IPV4}=true")

        val options = ctx.jvmArguments ++ username ++
          logArgs ++ remoteDebugConfig ++ verboseGCConfig ++ ipv4 ++ configArgs

        val process = procLauncher.createProcess(appId, executorId, resource, executorConfig,
          options, classPath, ctx.mainClass, ctx.arguments)

        ProcessInfo(process, jarPath, configFile)
      }

      new ExecutorHandler {

        var destroyed = false

        override def destroy(): Unit = {
          LOG.info(s"Destroy executor process ${ctx.mainClass}")
          if (!destroyed) {
            destroyed = true
            process.foreach { info =>
              info.process.destroy()
              info.jarPath.foreach(new File(_).delete())
              new File(info.configFile).delete()
            }
          }
        }

        override def exitValue: Future[Int] = {
          process.flatMap { info =>
            val exit = info.process.exitValue()
            if (exit == 0) {
              Future.successful(0)
            } else {
              Future.failed[Int](new Exception(s"Executor exit with failure, exit value: $exit, " +
              s"error summary: ${info.process.logger.error}"))
            }
          }
        }
      }
    }

    private def expandEnviroment(path: String): String = {
      // TODO: extend this to support more environment.
      path.replace(s"<${GEARPUMP_HOME}>", executorConfig.getString(GEARPUMP_HOME))
    }

    override def preStart(): Unit = {
      executorHandler.exitValue.onComplete { value =>
        procLauncher.cleanProcess(appId, executorId)
        val result = ExecutorResult(value)
        self ! result
      }
    }

    override def postStop(): Unit = {
      executorHandler.destroy()
    }

    // The folders are under ${GEARPUMP_HOME}
    val daemonPathPattern = List("lib" + File.separator + "yarn")

    override def receive: Receive = {
      case ShutdownExecutor(appId, executorId, reason: String) =>
        executorHandler.destroy()
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

    private def filterOutDaemonLib(classPath: Array[String]): Array[String] = {
      classPath.filterNot(matchDaemonPattern(_))
    }

    private def matchDaemonPattern(path: String): Boolean = {
      daemonPathPattern.exists(path.contains(_))
    }
  }

  trait ExecutorHandler {
    def destroy(): Unit
    def exitValue: Future[Int]
  }

  case class ProcessInfo(process: RichProcess, jarPath: Option[String], configFile: String)

  /**
   * Starts the executor in  the same JVM as worker.
   */
  class InJvmExecutor(launch: LaunchExecutor, exit: Promise[Int])
    extends Daemon(launch.executorJvmConfig.arguments(0), launch.executorJvmConfig.arguments(1)) {
    private val exitCode = 0

    override val supervisorStrategy =
      OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
        case ex: Throwable =>
          LOG.error(s"system $name stopped ", ex)
          exit.failure(ex)
          Stop
      }

    override def postStop(): Unit = {
      if (!exit.isCompleted) {
        exit.success(exitCode)
      }
    }
  }
}