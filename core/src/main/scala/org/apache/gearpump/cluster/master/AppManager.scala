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

package org.apache.gearpump.cluster.master

import akka.actor._
import akka.pattern.ask
import com.typesafe.config.Config
import org.apache.gearpump._
import org.apache.gearpump.cluster.AppMasterToMaster._
import org.apache.gearpump.cluster.AppMasterToWorker._
import org.apache.gearpump.cluster.ClientToMaster._
import InMemoryKVService._
import MasterHAService._
import org.apache.gearpump.cluster.MasterToAppMaster._
import org.apache.gearpump.cluster.MasterToClient._
import org.apache.gearpump.cluster.WorkerToAppMaster._
import org.apache.gearpump.cluster._
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util._
import org.slf4j.Logger

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * AppManager is dedicated part of Master to manager applicaitons
 */
private[cluster] class AppManager(masterHA : ActorRef, kvService: ActorRef, launcher: AppMasterLauncherFactory) extends Actor with Stash with TimeOutScheduler{
  private val LOG: Logger = LogUtil.getLogger(getClass)

  private val executorId : Int = APPMASTER_DEFAULT_EXECUTOR_ID
  private val systemconfig = context.system.settings.config
  private val appMasterMaxRetries: Int = 5
  private val appMasterRetryTimeRange: Duration = 20 seconds

  implicit val timeout = FUTURE_TIMEOUT
  implicit val executionContext = context.dispatcher

  //next available appId
  private var appId: Int = 0

  //from appid to appMaster data
  private var appMasterRegistry = Map.empty[Int, (ActorRef, AppMasterRuntimeInfo)]

  // dead appmaster list
  private var deadAppMasters = Map.empty[Int, (ActorRef, AppMasterRuntimeInfo)]

  private var appMasterRestartPolicies = Map.empty[Int, RestartPolicy]

  def receive: Receive = null

  masterHA ! GetMasterState
  context.become(waitForMasterState)

  def waitForMasterState: Receive = {
    case MasterState(maxId, state) =>
      this.appId = maxId + 1
      context.become(receiveHandler)
      unstashAll()
    case GetMasterStateFailed(ex) =>
      LOG.error("Failed to get master state, shutting down master to avoid data corruption...")
      context.parent ! PoisonPill
    case msg =>
      LOG.info(s"Get message ${msg.getClass.getSimpleName}")
      stash()
  }

  def receiveHandler : Receive = {
    val msg = "Application Manager started. Ready for application submission..."
    System.out.println(msg)
    LOG.info(msg)

    clientMsgHandler orElse appMasterMessage orElse selfMsgHandler orElse workerMessage orElse appDataStoreService orElse terminationWatch
  }

  def clientMsgHandler: Receive = {
    case SubmitApplication(app, jar, username) =>
      LOG.info(s"AppManager Submiting Application $appId...")
      val client = sender
      if (applicationNameExist(app.name)) {
        client ! SubmitApplicationResult(Failure(new Exception(s"Application name ${app.name} already existed")))
      } else {
        val appLauncher = context.actorOf(launcher.props(appId, executorId, app, jar, username, context.parent, Some(client)), s"launcher${appId}_${Util.randInt}")

        val appState = new ApplicationState(appId, app.name, 0, app, jar, username, null)
        appMasterRestartPolicies += appId -> new RestartPolicy(appMasterMaxRetries, appMasterRetryTimeRange)
        masterHA ! UpdateMasterState(appState)
        appId += 1
      }
    case ShutdownApplication(appId) =>
      LOG.info(s"App Manager Shutting down application $appId")
      val (appMaster, info) = appMasterRegistry.getOrElse(appId, (null, null))
      Option(info) match {
        case Some(info) =>
          val worker = info.worker
          LOG.info(s"Shutdown app master at ${Option(worker).map(_.path).orNull}, appId: $appId, executorId: $executorId")
          cleanApplicationData(appId)
          val shutdown = ShutdownExecutor(appId, executorId, s"AppMaster $appId shutdown requested by master...")
          sendMsgWithTimeOutCallBack(worker, shutdown, 30, shutDownExecutorTimeOut())
          sender ! ShutdownApplicationResult(Success(appId))
        case None =>
          val errorMsg = s"Failed to find regisration information for appId: $appId"
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

    case ResolveAppId(appId) =>
      LOG.info(s"App Manager Resolving appId $appId to ActorRef")
      val (appMaster, _) = appMasterRegistry.getOrElse(appId, (null, null))
      if (null != appMaster) {
        sender ! ResolveAppIdResult(Success(appMaster))
      } else {
        val errorMsg = s"Can not find regisration information for appId: $appId"
        LOG.error(errorMsg)
        sender ! ResolveAppIdResult(Failure(new Exception(errorMsg)))
      }
    case AppMastersDataRequest =>
      var appMastersData = collection.mutable.ListBuffer[AppMasterData]()
      appMasterRegistry.foreach(pair => {
        val (id, (appMaster:ActorRef, info: AppMasterRuntimeInfo)) = pair
        val appMasterPath = ActorUtil.getFullPath(context.system, appMaster.path)
        val workerPath = Option(info.worker).map(worker => ActorUtil.getFullPath(context.system, worker.path))
        appMastersData += AppMasterData(
          AppMasterActive, id, info.appName, appMasterPath, workerPath.orNull,
          info.submissionTime, info.startTime, info.finishTime, info.user)
      })

      deadAppMasters.foreach(pair => {
        val (id, (appMaster:ActorRef, info:AppMasterRuntimeInfo)) = pair
        val appMasterPath = ActorUtil.getFullPath(context.system, appMaster.path)
        val workerPath = Option(info.worker).map(worker => ActorUtil.getFullPath(context.system, worker.path))

        appMastersData += AppMasterData(
          AppMasterInActive, id, info.appName, appMasterPath, workerPath.orNull,
          info.submissionTime, info.startTime, info.finishTime, info.user)
      })

      sender ! AppMastersData(appMastersData.toList)
    case QueryAppMasterConfig(appId) =>
      val config =
        if (appMasterRegistry.contains(appId)) {
          val (appMaster, info) = appMasterRegistry(appId)
          info.config
        } else if (deadAppMasters.contains(appId)) {
          val (appMaster, info) = deadAppMasters(appId)
          info.config
        } else {
          null
        }
      sender ! AppMasterConfig(config)

    case appMasterDataRequest: AppMasterDataRequest =>
      val appId = appMasterDataRequest.appId

      val (appStatus, appMaster, info) =
        if (appMasterRegistry.contains(appId)) {
          val (appMaster, info) = appMasterRegistry(appId)
          (AppMasterActive, appMaster, info)
        } else if (deadAppMasters.contains(appId)) {
          val (appMaster, info) = deadAppMasters(appId)
          (AppMasterInActive, appMaster, info)
        } else {
          (AppMasterNonExist, null, null)
        }

      appStatus match {
        case AppMasterActive | AppMasterInActive =>
          val worker = info.worker
          val appMasterPath = ActorUtil.getFullPath(context.system, appMaster.path)
          val workerPath = Option(info.worker).map(
            worker => ActorUtil.getFullPath(context.system, worker.path)).orNull

          sender ! AppMasterData(
            appStatus, appId, info.appName, appMasterPath, workerPath,
            info.submissionTime, info.startTime, info.finishTime, info.user)

        case AppMasterNonExist =>
          sender ! AppMasterData(AppMasterNonExist)
      }

    case appMasterDataDetailRequest: AppMasterDataDetailRequest =>
      val appId = appMasterDataDetailRequest.appId
      val (appMaster, info) = appMasterRegistry.getOrElse(appId, (null, null))
      Option(appMaster) match {
        case Some(_appMaster) =>
          _appMaster forward appMasterDataDetailRequest
        case None =>
          sender ! GeneralAppMasterDataDetail(appId)
      }
    case appMasterMetricsRequest: AppMasterMetricsRequest =>
      val appId = appMasterMetricsRequest.appId
      val (appMaster, info) = appMasterRegistry.getOrElse(appId, (null, null))
      Option(appMaster) match {
        case Some(_appMaster) =>
          _appMaster forward appMasterMetricsRequest
        case None =>
      }

    case query@ QueryHistoryMetrics(appId, _) =>
      val (appMaster, info) = appMasterRegistry.getOrElse(appId, (null, null))
      Option(appMaster) match {
        case Some(_appMaster) =>
          _appMaster forward query
        case None =>
      }
    case appMasterMetricsRequest: AppMasterMetricsRequest =>
      val appId = appMasterMetricsRequest.appId
      val (appMaster, info) = appMasterRegistry.getOrElse(appId, (null, null))
      Option(appMaster) match {
        case Some(_appMaster) =>
          _appMaster forward appMasterMetricsRequest
        case None =>
      }

    case query@ QueryHistoryMetrics(appId, _) =>
      val (appMaster, info) = appMasterRegistry.getOrElse(appId, (null, null))
      Option(appMaster) match {
        case Some(_appMaster) =>
          _appMaster forward query
        case None =>
      }
    case appMasterMetricsRequest: AppMasterMetricsRequest =>
      val appId = appMasterMetricsRequest.appId
      val (appMaster, info) = appMasterRegistry.getOrElse(appId, (null, null))
      Option(appMaster) match {
        case Some(_appMaster) =>
          _appMaster forward appMasterMetricsRequest
        case None =>
      }

  }

  def workerMessage: Receive = {
    case ShutdownExecutorSucceed(appId, executorId) =>
      LOG.info(s"Shut down executor $executorId for application $appId successfully")
    case failed: ShutdownExecutorFailed =>
      LOG.error(failed.reason)
  }
  
  private def shutDownExecutorTimeOut(): Unit = {
    LOG.error(s"Shut down executor time out")
  }

  def appMasterMessage: Receive = {
    case RegisterAppMaster(appMaster, registerBack: AppMasterRuntimeInfo) =>
      val startTime = System.currentTimeMillis()
      val register = registerBack.copy(startTime = startTime)

      LOG.info(s"Register AppMaster for app: ${register.appId} $register")
      context.watch(appMaster)
      appMasterRegistry += register.appId -> (appMaster, register)
      sender ! AppMasterRegistered(register.appId)
  }

  def appDataStoreService: Receive = {
    case SaveAppData(appId, key, value) =>
      val client = sender
      (kvService ? PutKV(appId.toString, key, value)).asInstanceOf[Future[PutKVResult]].map {
        case PutKVSuccess =>
          client ! AppDataSaved
        case PutKVFailed(ex) =>
          client ! SaveAppDataFailed
      }
    case GetAppData(appId, key) =>
      val client = sender

      (kvService ? GetKV(appId.toString, key)).asInstanceOf[Future[GetKVResult]].map {
        case GetKVSuccess(privateKey, value) =>
          client ! GetAppDataResult(key, value)
        case GetKVFailed(ex) =>
          client ! GetAppDataResult(key, null)
      }
  }

  def terminationWatch: Receive = {
    case terminate: Terminated =>
      terminate.getAddressTerminated()
      LOG.info(s"App Master is terminiated, network down: ${terminate.getAddressTerminated()}")
      //Now we assume that the only normal way to stop the application is submitting a ShutdownApplication request
      val application = appMasterRegistry.find{appInfo =>
        val (_, (actorRef, _)) = appInfo
        actorRef.compareTo(terminate.actor) == 0
      }
      if(application.nonEmpty){
        val appId = application.get._1
        (masterHA ? GetMasterState).asInstanceOf[Future[GetMasterStateResult]].map {
          case MasterState(maxId, state) =>
            val appState = state.find(_.appId == appId)
            if (appState.isDefined) {
              LOG.info(s"Recovering application, $appId")
              self ! RecoverApplication(appState.get)
            } else {
              LOG.error(s"Cannot find application state for $appId")
            }
          case GetMasterStateFailed(ex) =>
            LOG.error(s"Cannot find master state to recover")
        }
      }
  }

  def selfMsgHandler: Receive = {
    case RecoverApplication(state) =>
      val appId = state.appId
      if(appMasterRestartPolicies.get(appId).get.allowRestart) {
        LOG.info(s"AppManager Recovering Application $appId...")
        context.actorOf(launcher.props(appId, executorId, state.app, state.jar, state.username, context.parent, None), s"launcher${appId}_${Util.randInt}")
      } else {
        LOG.error(s"Application $appId failed to many times")
      }
  }

  case class RecoverApplication(applicationStatus : ApplicationState)

  private def cleanApplicationData(appId : Int) : Unit = {

    //add the dead app to dead appMaster
    appMasterRegistry.get(appId).map { pair =>
      val (appMasterActor, info) = pair
      deadAppMasters += appId -> (appMasterActor, info.copy(finishTime = System.currentTimeMillis()))
    }

    appMasterRegistry -= appId

    masterHA ! DeleteMasterState(appId)
    kvService ! DeleteKVGroup(appId.toString)
  }

  private def applicationNameExist(appName: String): Boolean = {
    appMasterRegistry.values.exists(_._2.appName == appName)
  }
}

case class AppMasterRuntimeInfo(
    appId: Int,
    // appName is the unique Id for an application
    appName: String,
    worker : ActorRef = null,
    user: String = null,
    submissionTime: TimeStamp = 0,
    startTime: TimeStamp = 0,
    finishTime: TimeStamp = 0,
    config: Config = null)
  extends AppMasterRegisterData

object AppManager {

}
