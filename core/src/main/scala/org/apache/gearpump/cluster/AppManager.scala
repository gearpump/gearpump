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

package org.apache.gearpump.cluster

import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.cluster.Cluster
import akka.contrib.datareplication.Replicator._
import akka.contrib.datareplication.{DataReplication, GSet, LWWMap}
import akka.pattern.ask
import org.apache.gearpump.cluster.AppMasterToMaster._
import org.apache.gearpump.cluster.AppMasterToWorker._
import org.apache.gearpump.cluster.ClientToMaster._
import org.apache.gearpump.cluster.MasterToAppMaster._
import org.apache.gearpump.cluster.MasterToClient.{ReplayApplicationResult, ShutdownApplicationResult, SubmitApplicationResult}
import org.apache.gearpump.cluster.WorkerToAppMaster._
import org.apache.gearpump.cluster.scheduler.{Resource, ResourceRequest}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.ActorSystemBooter.{ActorCreated, BindLifeCycle, CreateActor, RegisterActorSystem}
import org.apache.gearpump.util._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
/**
 * AppManager is dedicated part of Master to manager applicaitons
 */

/**
 * This state will be persisted across the masters.
 */
class ApplicationState(val appId : Int, val attemptId : Int, val app : Application, val jar: Option[AppJar], state : Any) extends Serializable {

  override def equals(other: Any): Boolean = {
    other match {
      case that: ApplicationState =>
        if (appId == that.appId && attemptId == that.attemptId) {
          true
        } else {
          false
        }
      case _ =>
        false
    }
  }

  override def hashCode: Int = {
    import akka.routing.MurmurHash._
    extendHash(appId, attemptId, startMagicA, startMagicB)
  }
}

private[cluster] class AppManager() extends Actor with Stash {

  import context.dispatcher
  import org.apache.gearpump.cluster.AppManager._
  implicit val timeout = Constants.FUTURE_TIMEOUT

  private var master: ActorRef = null
  private var executorCount: Int = 0
  private var appId: Int = 0

  private val systemconfig = context.system.settings.config

  def receive: Receive = null

  //from appid to appMaster data
  private var appMasterRegistry = Map.empty[Int, Either[(ActorRef, AppMasterInfo), InvalidAppMaster]]

  private val STATE = "masterstate"
  private val TIMEOUT = Duration(5, TimeUnit.SECONDS)
  private val replicator = DataReplication(context.system).replicator
  implicit val cluster = Cluster(context.system)

  //TODO: We can use this state for appmaster HA to recover a new App master
  private var state: Set[ApplicationState] = Set.empty[ApplicationState]

  val masterClusterSize = systemconfig.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).size()

  //optimize write path, we can tolerate one master down for recovery.
  val writeQuorum = Math.min(2, masterClusterSize / 2 + 1)
  val readQuorum = masterClusterSize + 1 - writeQuorum

  replicator ! new Get(STATE, ReadFrom(readQuorum), TIMEOUT, None)

  override def preStart(): Unit = {
    replicator ! Subscribe(STATE, self)
  }

  override def postStop(): Unit = {
    replicator ! Unsubscribe(STATE, self)
  }

  LOG.info("Recovering application state....")
  context.become(waitForMasterState)

  def waitForMasterState: Receive = {
    case GetSuccess(_, replicatedState: GSet, _) =>
      state = replicatedState.getValue().asScala.foldLeft(state) { (set, appState) =>
        set + appState.asInstanceOf[ApplicationState]
      }
      appId = state.map(_.appId).size
      LOG.info(s"Successfully received application states for ${state.map(_.appId)}, nextAppId: $appId....")
      context.become(receiveHandler)
      unstashAll()
    case x: GetFailure =>
      LOG.info("GetFailure We cannot find any existing state, start a fresh one...")
      context.become(receiveHandler)
      unstashAll()
    case x: NotFound =>
      LOG.info("We cannot find any existing state, start a fresh one...")
      context.become(receiveHandler)
      unstashAll()
    case msg =>
      LOG.info(s"Get information ${msg.getClass.getSimpleName}")
      stash()
  }

  def receiveHandler = masterHAMsgHandler orElse clientMsgHandler orElse appMasterMessage orElse selfMsgHandler orElse appDataStoreService orElse terminationWatch

  def masterHAMsgHandler: Receive = {
    case update: UpdateResponse => LOG.info(s"we get update $update")
    case Changed(STATE, data: GSet) =>
      LOG.info("Current elements: {}", data.value)
  }

  def clientMsgHandler: Receive = {
    case submitApp@SubmitApplication(app, jar) =>
      LOG.info(s"AppManager Submiting Application $appId...")
      val appWatcher = context.actorOf(Props(classOf[AppMasterStarter], appId, app, jar), appId.toString)

      LOG.info(s"Persist master state writeQuorum: $writeQuorum, timeout: $TIMEOUT...")
      val appState = new ApplicationState(appId, 0, app, jar, null)
      replicator ! Update(STATE, GSet(), WriteTo(writeQuorum), TIMEOUT)(_ + appState)
      sender.tell(SubmitApplicationResult(Success(appId)), context.parent)
      appId += 1
    case ShutdownApplication(appId) =>
      LOG.info(s"App Manager Shutting down application $appId")
      val maybeEither = appMasterRegistry.get(appId)
      maybeEither match {
        case Some(either) =>
          if (either.isLeft) {
            val (appMaster, info) = either.left.get
            val worker = info.worker
            LOG.info(s"Shuttdown app master at ${worker.path}, appId: $appId, executorId: $masterExecutorId")
            cleanApplicationData(appId)
            worker ! ShutdownExecutor(appId, masterExecutorId, s"AppMaster $appId shutdown requested by master...")
            sender ! ShutdownApplicationResult(Success(appId))
          } else {
            val invalidAppMaster = either.right.get
            val errorMsg = s"Invalid AppMaster ${invalidAppMaster.appMaster}"
            sender ! ShutdownApplicationResult(Failure(new Exception(errorMsg)))
          }
        case None =>
          val errorMsg = s"Failed to find regisration information for appId: $appId"
          LOG.error(errorMsg)
          sender ! ShutdownApplicationResult(Failure(new Exception(errorMsg)))
      }
    case ReplayFromTimestampWindowTrailingEdge(appId) =>
      LOG.info(s"App Manager Replaying application $appId")
      val maybeEither = appMasterRegistry.get(appId)
      maybeEither match {
        case Some(either) =>
          if (either.isLeft) {
            val (appMaster, _) = either.left.get
            LOG.info(s"Replaying application: $appId")
            appMaster forward ReplayFromTimestampWindowTrailingEdge
            sender ! ReplayApplicationResult(Success(appId))
          } else {
            val invalidAppMaster = either.right.get
            val errorMsg = s"Invalid AppMaster ${invalidAppMaster.appMaster}"
            LOG.error(errorMsg)
            sender ! ReplayApplicationResult(Failure(new Exception(errorMsg)))
          }
        case None =>
          val errorMsg = s"Failed to find regisration information for appId: $appId"
          LOG.error(errorMsg)
          sender ! ReplayApplicationResult(Failure(new Exception(errorMsg)))
      }
  }

  def appMasterMessage: Receive = {
    case RegisterAppMaster(appMaster, appId, executorId, slots, registerData: AppMasterInfo) =>
      val appMasterPath = appMaster.path.address.toString
      val workerPath = registerData.worker.path.address.toString
      LOG.info(s"Register AppMaster for app: $appId appMaster=$appMasterPath worker=$workerPath")
      context.watch(appMaster)
      appMasterRegistry += appId -> Left((appMaster, registerData))
      sender ! AppMasterRegistered(appId, context.parent)
    case AppMastersDataRequest =>
      val appMastersData = collection.mutable.ListBuffer[AppMasterData]()
      appMasterRegistry.foreach(pair => {
        val (id, either) = pair
        if(either.isLeft) {
          val (appMaster: ActorRef, info: AppMasterInfo) = either.left.get
          appMastersData += AppMasterData(id, info)
        }
      }
      )
      sender ! AppMastersData(appMastersData.toList)
    case appMasterDataRequest: AppMasterDataRequest =>
      val appId = appMasterDataRequest.appId
      val maybeEither = appMasterRegistry.get(appId)
      maybeEither match {
        case Some(either) =>
          if (either.isLeft) {
            val (appMaster, info) = either.left.get
            val worker = info.worker
            sender ! AppMasterData(appId = appId, appData = info)
          } else {
            sender ! AppMasterData(appId = appId, appData = null)
          }
        case None =>
          sender ! AppMasterData(appId = appId, appData = null)
      }
    case appMasterDataDetailRequest: AppMasterDataDetailRequest =>
      val appId = appMasterDataDetailRequest.appId
      val maybeEither = appMasterRegistry.get(appId)
      maybeEither match {
        case Some(either) =>
          if (either.isLeft) {
            val (appMaster, info) = either.left.get
            val appM = appMaster
            val path = appM.toString
            LOG.info(s"AppManager forwarding AppMasterDataRequest to AppMaster $path")
            appM forward appMasterDataDetailRequest
          } else {
            sender ! AppMasterDataDetail(appId = appId, appDescription = null)
          }
        case None =>
          sender ! AppMasterDataDetail(appId = appId, appDescription = null)
      }
    case invalidAppMaster: InvalidAppMaster =>
      LOG.info(s"InvalidAppMaster adding to appMasterRegistry ${invalidAppMaster.appId}")
      appMasterRegistry += invalidAppMaster.appId -> Right(invalidAppMaster)
  }

  def appDataStoreService: Receive = {
    case SaveAppData(appId, key, value) =>
      val maybeEither = appMasterRegistry.get(appId)
      maybeEither match {
        case Some(either) =>
          if (either.isLeft) {
            val (_, info) = either.left.get
            if (info != null) {
              LOG.debug(s"saving application data $key for application $appId")
              replicator ! Update(appId.toString, LWWMap(), WriteTo(writeQuorum), TIMEOUT)(_ + (key -> value))
            } else {
              LOG.error(s"no match application for app$appId when saving application data")
            }
          } else {
            LOG.error(s"no match application for app$appId when saving application data")
          }
        case None =>
          LOG.error(s"no match application for app$appId when saving application data")
      }
      sender ! AppDataReceived
    case GetAppData(appId, key) =>
      val appMaster = sender
      (replicator ? new Get(appId.toString, ReadFrom(readQuorum), TIMEOUT, None)).asInstanceOf[Future[ReplicatorMessage]].map{
        case GetSuccess(_, appData: LWWMap, _) =>
          if(appData.get(key).nonEmpty){
            appMaster ! GetAppDataResult(key, appData.get(key).get)
          } else {
            appMaster ! GetAppDataResult(key, null)
          }
        case _ =>
          LOG.error(s"failed to get application $appId data, the request key is $key")
          appMaster ! GetAppDataResult(key, null)
      }
  }

  def terminationWatch: Receive = {
    case terminate: Terminated =>
      terminate.getAddressTerminated()
      LOG.info(s"App Master is terminiated, network down: ${terminate.getAddressTerminated()}")
      //Now we assume that the only normal way to stop the application is submitting a ShutdownApplication request
      val application = appMasterRegistry.find{pair =>
        val (appId, maybeAppInfo) = pair
        if(maybeAppInfo.isLeft) {
          val (actorRef, _) = maybeAppInfo.left.get
          actorRef.compareTo(terminate.actor) == 0
        } else {
          false
        }
      }
      if(application.nonEmpty){
        val appId = application.get._1
        (replicator ? new Get(STATE, ReadFrom(readQuorum), TIMEOUT, None)).asInstanceOf[Future[ReplicatorMessage]].map{
          case GetSuccess(_, replicatedState: GSet, _) =>
            val appState = replicatedState.value.find(_.asInstanceOf[ApplicationState].appId == appId)
            if(appState.nonEmpty){
              self ! RecoverApplication(appState.get.asInstanceOf[ApplicationState])
            }
          case _ =>
            LOG.error(s"failed to recover application $appId, can not find application state")
        }
      }
  }

  def selfMsgHandler: Receive = {
    case RecoverApplication(applicationStatus) =>
      val appId = applicationStatus.appId
      LOG.info(s"AppManager Recovering Application $appId...")
      context.actorOf(Props(classOf[AppMasterStarter], appId, applicationStatus.app.conf, applicationStatus.jar, applicationStatus.app), appId.toString)
  }

  case class RecoverApplication(applicationStatus : ApplicationState)

  private def cleanApplicationData(appId : Int) : Unit = {
    appMasterRegistry -= appId
    replicator ! Update(STATE, GSet(), WriteTo(writeQuorum), TIMEOUT)(set =>
      GSet(set.value.filter(_.asInstanceOf[ApplicationState].appId != appId)))
    replicator ! Delete(appId.toString)
  }
}

case class AppMasterInfo(worker : ActorRef) extends AppMasterRegisterData


private[cluster] object AppManager {
  private val masterExecutorId = -1
  private val LOG: Logger = LoggerFactory.getLogger(classOf[AppManager])

  /**
   * Start and watch Single AppMaster's lifecycle
   */
  class AppMasterStarter(appId : Int, app : Application, jar: Option[AppJar]) extends Actor {

    val systemConfig = context.system.settings.config

    val master = context.actorSelection("../../")
    master ! RequestResource(appId, ResourceRequest(Resource(1)))
    LOG.info(s"AppManager asking Master for resource for app $appId...")

    def receive : Receive = waitForResourceAllocation

    def waitForResourceAllocation : Receive = {
      case ResourceAllocated(allocations) =>
        LOG.info(s"Resource allocated for appMaster $app Id")
        val allocation = allocations(0)
        val appMasterConfig = app.conf.withAppId(appId).withAppDescription(app).withAppMasterRegisterData(AppMasterInfo(allocation.worker)).withExecutorId(masterExecutorId).withResource(allocation.resource)
        LOG.info(s"Try to launch a executor for app Master on ${allocation.worker} for app $appId")
        val name = ActorUtil.actorNameForExecutor(appId, masterExecutorId)
        val selfPath = ActorUtil.getFullPath(context)
        val maybeExtraClasspath = app.conf.config.get(Constants.GEARPUMP_APPMASTER_EXTRA_CLASSPATH)
        val extraClasspath = maybeExtraClasspath.getOrElse("").asInstanceOf[String]
        val classPath = Array.concat(Util.getCurrentClassPath,  extraClasspath.split(File.pathSeparator))
        val executionContext = ExecutorContext(classPath, appMasterConfig.getString(Constants.GEARPUMP_APPMASTER_ARGS).split(" "), classOf[ActorSystemBooter].getName, Array(name, selfPath), jar)

        allocation.worker ! LaunchExecutor(appId, masterExecutorId, allocation.resource, executionContext)
        context.become(waitForActorSystemToStart(allocation.worker, appMasterConfig))
    }

    def waitForActorSystemToStart(worker : ActorRef, masterConfig : Configs) : Receive = {
      case ExecutorLaunchRejected(reason, resource, ex) =>
        LOG.error(s"Executor Launch failed reason：$reason", ex)
        LOG.info(s"reallocate resource $resource to start appmaster")
        master ! RequestResource(appId, ResourceRequest(resource))
      case RegisterActorSystem(systemPath) =>
        LOG.info(s"Received RegisterActorSystem $systemPath for app master")
        //bind lifecycle with worker
        sender ! BindLifeCycle(worker)
        val masterAddress = systemConfig.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).asScala.map { address =>
          val hostAndPort = address.split(":")
          HostPort(hostAndPort(0), hostAndPort(1).toInt)
        }
        LOG.info(s"Create master proxy on target actor system $systemPath")
        val masterProxyConfig = Props(classOf[MasterProxy], masterAddress)
        sender ! CreateActor(masterProxyConfig, "masterproxy")
        context.become(waitForMasterProxyToStart(worker, masterConfig))
    }

    def waitForMasterProxyToStart(worker: ActorRef, masterConfig : Configs) : Receive = {
      case ActorCreated(masterProxy, "masterproxy") =>
        LOG.info(s"Master proxy is created, create appmaster...")
        val appMasterClass = Try(Class.forName(app.appMaster).asSubclass(classOf[ApplicationMaster]))
        appMasterClass match {
          case Success(clazz) =>
            val masterProps = Props(clazz, masterConfig.withMasterProxy(masterProxy))
            sender ! CreateActor(masterProps, "appmaster")
          case Failure(e) =>
            worker ! ShutdownExecutor(appId, masterExecutorId, e.getMessage)
            master ! InvalidAppMaster(appId, app.appMaster)
        }

        //my job has completed. kill myself
        self ! PoisonPill
    }
  }
}
