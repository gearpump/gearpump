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

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.cluster.Cluster
import akka.contrib.datareplication.Replicator._
import akka.contrib.datareplication.{LWWMap, DataReplication, GSet}
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
import scala.util.{Failure, Success}
/**
 * AppManager is dedicated part of Master to manager applicaitons
 */

/**
 * This state will be persisted across the masters.
 */
class ApplicationState(val appId : Int, val attemptId : Int, val appMasterClass : Class[_ <: Actor], val app : Application, val state : Any) extends Serializable {

  override def equals(other: Any): Boolean = {
    if (other.isInstanceOf[ApplicationState]) {
      val that = other.asInstanceOf[ApplicationState]
      if (appId == that.appId && attemptId == that.attemptId && appMasterClass.equals(that.appMasterClass) && app.equals(that.app)) {
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  override def hashCode: Int = {
    import akka.routing.MurmurHash._
    extendHash(appId, attemptId, startMagicA, startMagicB)
  }
}

private[cluster] class AppManager() extends Actor with Stash {

  import org.apache.gearpump.cluster.AppManager._

  private var master: ActorRef = null
  private var executorCount: Int = 0
  private var appId: Int = 0

  private val systemconfig = context.system.settings.config

  def receive: Receive = null

  //from appid to appMaster data
  private var appMasterRegistry = Map.empty[Int, (ActorRef, AppMasterInfo)]

  private val STATE = "masterstate"
  private val TIMEOUT = Duration(5, TimeUnit.SECONDS)
  private val replicator = DataReplication(context.system).replicator
  implicit val cluster = Cluster(context.system)

  //TODO: We can use this state for appmaster HA to recover a new App master
  private var state: Set[ApplicationState] = Set.empty[ApplicationState]

  val masterClusterSize = systemconfig.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).size()

  //optimize write path, we can tollerate one master down for recovery.
  val writeQuorum = Math.min(2, masterClusterSize / 2 + 1)
  val readQuorum = masterClusterSize + 1 - writeQuorum

  replicator ! new Get(STATE, ReadFrom(readQuorum), TIMEOUT, None)

  override def preStart: Unit = {
    replicator ! Subscribe(STATE, self)
  }

  override def postStop: Unit = {
    replicator ! Unsubscribe(STATE, self)
  }

  LOG.info("Recoving application state....")
  context.become(waitForMasterState)

  def waitForMasterState: Receive = {
    case GetSuccess(_, replicatedState: GSet, _) =>
      state = replicatedState.getValue().asScala.foldLeft(state) { (set, appState) =>
        set + appState.asInstanceOf[ApplicationState]
      }
      appId = state.map(_.appId).size
      LOG.info(s"Successfully recoeved application states for ${state.map(_.appId)}, nextAppId: ${appId}....")
      context.become(receiveHandler)
      unstashAll()
    case x: GetFailure =>
      LOG.info("GetFailure We cannot find any exisitng state, start a fresh one...")
      context.become(receiveHandler)
      unstashAll()
    case x: NotFound =>
      LOG.info("We cannot find any exisitng state, start a fresh one...")
      context.become(receiveHandler)
      unstashAll()
    case msg =>
      LOG.info(s"Get information ${msg.getClass.getSimpleName}")
      stash()
  }

  def receiveHandler = masterHAMsgHandler orElse clientMsgHandler orElse appMasterMessage orElse terminationWatch orElse selfMsgHandler

  def masterHAMsgHandler: Receive = {
    case update: UpdateResponse => LOG.info(s"we get update $update")
    case Changed(STATE, data: GSet) =>
      LOG.info("Current elements: {}", data.value)
  }

  def clientMsgHandler: Receive = {
    case submitApp@SubmitApplication(appMasterClass, config, app) =>
      LOG.info(s"AppManager Submiting Application $appId...")
      val appWatcher = context.actorOf(Props(classOf[AppMasterStarter], appId, appMasterClass, config, app), appId.toString)

      LOG.info(s"Persist master state writeQuorum: ${writeQuorum}, timeout: ${TIMEOUT}...")
      val appState = new ApplicationState(appId, 0, appMasterClass, app, null)
      replicator ! Update(STATE, GSet(), WriteTo(writeQuorum), TIMEOUT)(_ + appState)
      sender.tell(SubmitApplicationResult(Success(appId)), context.parent)
      appId += 1
    case ShutdownApplication(appId) =>
      LOG.info(s"App Manager Shutting down application $appId")
      val (appMaster, info) = appMasterRegistry.getOrElse(appId, (null, null))
      Option(info) match {
        case Some(info) =>
          val worker = info.worker
          LOG.info(s"Shuttdown app master at ${worker.path}, appId: $appId, executorId: $masterExecutorId")
          cleanApplicationData(appId)
          worker ! ShutdownExecutor(appId, masterExecutorId, s"AppMaster $appId shutdown requested by master...")
          sender ! ShutdownApplicationResult(Success(appId))
        case None =>
          val errorMsg = s"Find to find regisration information for appId: $appId"
          LOG.error(errorMsg)
          sender ! ShutdownApplicationResult(Failure(new Exception(errorMsg)))
      }
    case ReplayFromTimestampWindowTrailingEdge(appId) =>
      LOG.info(s"App Manager Replaying application $appId")
      val (appMaster, _) = appMasterRegistry.getOrElse(appId, (null, null))
      Option(appMaster) match {
        case Some(ref) =>
          LOG.info(s"Replaying application: $appId")
          ref forward ReplayFromTimestampWindowTrailingEdge
          sender ! ReplayApplicationResult(Success(appId))
        case None =>
          val errorMsg = s"Can not find regisration information for appId: $appId"
          LOG.error(errorMsg)
          sender ! ReplayApplicationResult(Failure(new Exception(errorMsg)))
      }
  }

  implicit val timeout = akka.util.Timeout(3, TimeUnit.SECONDS)
  import context.dispatcher

  def appMasterMessage: Receive = {
    case RegisterAppMaster(appMaster, appId, executorId, slots, registerData: AppMasterInfo) =>
      val appMasterPath = appMaster.path.address.toString
      val workerPath = registerData.worker.path.address.toString
      LOG.info(s"Register AppMaster for app: $appId appMaster=$appMasterPath worker=$workerPath")
      context.watch(appMaster)
      appMasterRegistry += appId -> (appMaster, registerData)
      sender ! AppMasterRegistered(appId, context.parent)
    case AppMastersDataRequest =>
      val appMastersData = collection.mutable.ListBuffer[AppMasterData]()
      appMasterRegistry.foreach(pair => {
        val (id, (appMaster:ActorRef, info:AppMasterInfo)) = pair
        appMastersData += AppMasterData(id,info)
      }
      )
      sender ! AppMastersData(appMastersData.toList)
    case appMasterDataRequest: AppMasterDataRequest =>
      val appId = appMasterDataRequest.appId
      val (appMaster, info) = appMasterRegistry.getOrElse(appId, (null, null))
      Option(info) match {
        case a@Some(data) =>
          val worker = a.get.worker
          sender ! AppMasterData(appId = appId, appData = data)
        case None =>
          sender ! AppMasterData(appId = appId, appData = null)
      }
    case appMasterDataDetailRequest: AppMasterDataDetailRequest =>
      val appId = appMasterDataDetailRequest.appId
      val (appMaster, info) = appMasterRegistry.getOrElse(appId, (null, null))
      Option(appMaster) match {
        case a@Some(appMaster) =>
          val appM:ActorRef = a.get
          val path = appM.toString
          LOG.info(s"AppManager forwarding AppMasterDataRequest to AppMaster $path")
          appM forward appMasterDataDetailRequest
        case None =>
          sender ! AppMasterDataDetail(appId = appId, appDescription = null)
      }
    case PostAppData(appId, key, value) =>
      val (_, info) = appMasterRegistry.getOrElse(appId, (null, null))
      Option(info) match {
        case a@Some(data) =>
          LOG.debug(s"saving application data $key for application $appId")
          replicator ! Update(appId.toString, LWWMap(), WriteTo(writeQuorum), TIMEOUT)(_ + (key -> value))
        case None =>
          LOG.error(s"no match application for app$appId when saving application data")
      }
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
    case terminate: Terminated => {
      terminate.getAddressTerminated()
      LOG.info(s"App Master is terminiated, network down: ${terminate.getAddressTerminated()}")
      //Now we assume that the only normal way to stop the application is submitting a ShutdownApplication request
      val application = appMasterRegistry.find{param =>
        val (_, (actorRef, _)) = param
        actorRef.compareTo(terminate.actor) == 0
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
  }

  def selfMsgHandler: Receive = {
    case RecoverApplication(applicationStatus) =>
      val terminatedAppId = applicationStatus.appId
      LOG.info(s"AppManager Recovering Application $terminatedAppId...")
      val appMasterClass = applicationStatus.appMasterClass
      context.actorOf(Props(classOf[AppMasterStarter], terminatedAppId, appMasterClass, Configs.empty, applicationStatus.app), terminatedAppId.toString)
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
  class AppMasterStarter(appId : Int, appMasterClass : Class[_ <: Actor], appConfig : Configs, app : Application) extends Actor {

    val systemConfig = context.system.settings.config

    val master = context.actorSelection("../../")
    master ! RequestResource(appId, ResourceRequest(Resource(1)))
    LOG.info(s"AppManager asking Master for resource for app $appId...")

    def receive : Receive = waitForResourceAllocation

    def waitForResourceAllocation : Receive = {
      case ResourceAllocated(allocations) => {
        LOG.info(s"Resource allocated for appMaster $app Id")
        val allocation = allocations(0)
        val appMasterConfig = appConfig.withAppId(appId).withAppDescription(app).withAppMasterRegisterData(AppMasterInfo(allocation.worker)).withExecutorId(masterExecutorId).withResource(allocation.resource)
        LOG.info(s"Try to launch a executor for app Master on ${allocation.worker} for app $appId")
        val name = actorNameForExecutor(appId, masterExecutorId)
        val selfPath = ActorUtil.getFullPath(context)

        val executionContext = ExecutorContext(Util.getCurrentClassPath, context.system.settings.config.getString(Constants.GEARPUMP_APPMASTER_ARGS).split(" "), classOf[ActorSystemBooter].getName, Array(name, selfPath))

        allocation.worker ! LaunchExecutor(appId, masterExecutorId, allocation.resource, executionContext)
        context.become(waitForActorSystemToStart(allocation.worker, appMasterConfig))
      }
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
        LOG.info(s"Create master proxy on target actor system ${systemPath}")
        val masterProxyConfig = Props(classOf[MasterProxy], masterAddress)
        sender ! CreateActor(masterProxyConfig, "masterproxy")
        context.become(waitForMasterProxyToStart(masterConfig))
    }

    def waitForMasterProxyToStart(masterConfig : Configs) : Receive = {
      case ActorCreated(masterProxy, "masterproxy") =>
        LOG.info(s"Master proxy is created, create appmaster...")
        val masterProps = Props(appMasterClass, masterConfig.withMasterProxy(masterProxy))
        sender ! CreateActor(masterProps, "appmaster")

        //my job has completed. kill myself
        self ! PoisonPill
    }

    private def actorNameForExecutor(appId : Int, executorId : Int) = "app" + appId + "-executor" + executorId
  }
}