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

package org.apache.gearpump.cluster.master

import akka.actor._
import akka.pattern.ask
import org.apache.gearpump.cluster.AppMasterToMaster.{AppDataSaved, SaveAppDataFailed, _}
import org.apache.gearpump.cluster.AppMasterToWorker._
import org.apache.gearpump.cluster.ClientToMaster._
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMasterDataRequest, AppMastersDataRequest, _}
import org.apache.gearpump.cluster.MasterToClient._
import org.apache.gearpump.cluster.WorkerToAppMaster.{ShutdownExecutorFailed, _}
import org.apache.gearpump.cluster.appmaster.{AppMasterRuntimeInfo, ApplicationState}
import org.apache.gearpump.cluster.master.AppManager._
import org.apache.gearpump.cluster.master.InMemoryKVService.{GetKVResult, PutKVResult, PutKVSuccess, _}
import org.apache.gearpump.cluster.master.Master._
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.{ActorUtil, TimeOutScheduler, Util, _}
import org.slf4j.Logger

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * AppManager is dedicated child of Master to manager all applications.
 */
private[cluster] class AppManager(kvService: ActorRef, launcher: AppMasterLauncherFactory)
  extends Actor with Stash with TimeOutScheduler {

  private val LOG: Logger = LogUtil.getLogger(getClass)

  private val EXECUTOR_ID: Int = APPMASTER_DEFAULT_EXECUTOR_ID
  private val appMasterMaxRetries: Int = 5
  private val appMasterRetryTimeRange: Duration = 20.seconds

  implicit val timeout = FUTURE_TIMEOUT
  implicit val executionContext = context.dispatcher

  // Next available appId
  private var nextAppId: Int = 1

  // From appId to appMaster data
  // Applications not in activeAppMasters or deadAppMasters are in pending status
  private var appMasterRegistry = Map.empty[Int, (ActorRef, AppMasterRuntimeInfo)]

  // Active appMaster list where applications are in active status
  private var activeAppMasters = Set.empty[Int]

  // Dead appMaster list where applications are in inactive status
  private var deadAppMasters = Set.empty[Int]

  private var appMasterRestartPolicies = Map.empty[Int, RestartPolicy]

  def receive: Receive = null

  kvService ! GetKV(MASTER_GROUP, MASTER_STATE)
  context.become(waitForMasterState)

  def waitForMasterState: Receive = {
    case GetKVSuccess(_, result) =>
      val masterState = result.asInstanceOf[MasterState]
      if (masterState != null) {
        this.nextAppId = masterState.maxId + 1
        this.activeAppMasters = masterState.activeAppMasters
        this.deadAppMasters = masterState.deadAppMasters
        this.appMasterRegistry = masterState.appMasterRegistry
      }
      context.become(receiveHandler)
      unstashAll()
    case GetKVFailed(ex) =>
      LOG.error("Failed to get master state, shutting down master to avoid data corruption...")
      context.parent ! PoisonPill
    case msg =>
      LOG.info(s"Get message ${msg.getClass.getSimpleName}")
      stash()
  }

  def receiveHandler: Receive = {
    val msg = "Application Manager started. Ready for application submission..."
    LOG.info(msg)
    clientMsgHandler orElse appMasterMessage orElse selfMsgHandler orElse workerMessage orElse
      appDataStoreService orElse terminationWatch
  }

  def clientMsgHandler: Receive = {
    case SubmitApplication(app, jar, username) =>
      LOG.info(s"Submit Application ${app.name}($nextAppId) by $username...")
      val client = sender()
      if (applicationNameExist(app.name)) {
        client ! SubmitApplicationResult(Failure(
          new Exception(s"Application name ${app.name} already existed")))
      } else {
        context.actorOf(launcher.props(nextAppId, EXECUTOR_ID, app, jar, username, context.parent,
          Some(client)), s"launcher${nextAppId}_${Util.randInt()}")

        val appState = new ApplicationState(nextAppId, app.name, 0, app, jar, username, null)
        appMasterRestartPolicies += nextAppId ->
          new RestartPolicy(appMasterMaxRetries, appMasterRetryTimeRange)
        kvService ! PutKV(nextAppId.toString, APP_STATE, appState)
        nextAppId += 1
      }

    case RestartApplication(appId) =>
      val client = sender()
      (kvService ? GetKV(appId.toString, APP_STATE)).asInstanceOf[Future[GetKVResult]].map {
        case GetKVSuccess(_, result) =>
          val appState = result.asInstanceOf[ApplicationState]
          if (appState != null) {
            LOG.info(s"Shutting down the application (restart), $appId")
            self ! ShutdownApplication(appId)
            self.tell(SubmitApplication(appState.app, appState.jar, appState.username), client)
          } else {
            client ! SubmitApplicationResult(Failure(
              new Exception(s"Failed to restart, because the application $appId does not exist.")
            ))
          }
        case GetKVFailed(ex) =>
          client ! SubmitApplicationResult(Failure(
            new Exception(s"Unable to obtain the Master State. " +
              s"Application $appId will not be restarted.")
          ))
      }

    case ShutdownApplication(appId) =>
      LOG.info(s"App Manager Shutting down application $appId")
      val (_, appInfo) = appMasterRegistry.get(appId)
        .filter { case (_, info) => !deadAppMasters.contains(info.appId)}
        .getOrElse((null, null))
      Option(appInfo) match {
        case Some(info) =>
          val worker = info.worker
          val workerPath = Option(worker).map(_.path).orNull
          LOG.info(s"Shutdown AppMaster at $workerPath, appId: $appId, executorId: $EXECUTOR_ID")
          cleanApplicationData(appId)
          val shutdown = ShutdownExecutor(appId, EXECUTOR_ID,
            s"AppMaster $appId shutdown requested by master...")
          sendMsgWithTimeOutCallBack(worker, shutdown, 30000, shutDownExecutorTimeOut())
          sender ! ShutdownApplicationResult(Success(appId))
        case None =>
          val errorMsg = s"Failed to find registration information for appId: $appId"
          LOG.error(errorMsg)
          sender ! ShutdownApplicationResult(Failure(new Exception(errorMsg)))
      }

    case ResolveAppId(appId) =>
      val (appMaster, _) = appMasterRegistry.getOrElse(appId, (null, null))
      if (null != appMaster) {
        sender ! ResolveAppIdResult(Success(appMaster))
      } else {
        sender ! ResolveAppIdResult(Failure(new Exception(s"Can not find Application: $appId")))
      }

    case AppMastersDataRequest =>
      var appMastersData = collection.mutable.ListBuffer[AppMasterData]()
      appMasterRegistry.foreach(pair => {
        val (id, (appMaster: ActorRef, info: AppMasterRuntimeInfo)) = pair
        val appMasterPath = ActorUtil.getFullPath(context.system, appMaster.path)
        val workerPath = Option(info.worker).map(worker =>
          ActorUtil.getFullPath(context.system, worker.path))
        val status = getAppMasterStatus(id)
        appMastersData += AppMasterData(
          status, id, info.appName, appMasterPath, workerPath.orNull,
          info.submissionTime, info.startTime, info.finishTime, info.user)
      })

      sender ! AppMastersData(appMastersData.toList)

    case QueryAppMasterConfig(appId) =>
      val config =
        if (appMasterRegistry.contains(appId)) {
          val (_, info) = appMasterRegistry(appId)
          info.config
        } else {
          null
        }
      sender ! AppMasterConfig(config)

    case appMasterDataRequest: AppMasterDataRequest =>
      val appId = appMasterDataRequest.appId
      val appStatus = getAppMasterStatus(appId)

      appStatus match {
        case AppMasterNonExist =>
          sender ! AppMasterData(AppMasterNonExist)
        case _ =>
          val (appMaster, info) = appMasterRegistry(appId)
          val appMasterPath = ActorUtil.getFullPath(context.system, appMaster.path)
          val workerPath = Option(info.worker).map(
            worker => ActorUtil.getFullPath(context.system, worker.path)).orNull
          sender ! AppMasterData(
            appStatus, appId, info.appName, appMasterPath, workerPath,
            info.submissionTime, info.startTime, info.finishTime, info.user)
      }
  }

  def workerMessage: Receive = {
    case ShutdownExecutorSucceed(appId, executorId) =>
      LOG.info(s"Shut down executor $executorId for application $appId successfully")
    case failed: ShutdownExecutorFailed =>
      LOG.error(failed.reason)
  }

  private def getAppMasterStatus(appId: Int): AppMasterStatus = {
    if (activeAppMasters.contains(appId)) {
      AppMasterActive
    } else if (deadAppMasters.contains(appId)) {
      AppMasterInActive
    } else if (appMasterRegistry.contains(appId)) {
      AppMasterPending
    } else {
      AppMasterNonExist
    }
  }

  private def shutDownExecutorTimeOut(): Unit = {
    LOG.error(s"Shut down executor time out")
  }

  def appMasterMessage: Receive = {
    case RegisterAppMaster(appMaster, registerBack: AppMasterRuntimeInfo) =>
      val startTime = System.currentTimeMillis()
      val register = registerBack.copy(startTime = startTime)

      LOG.info(s"Register AppMaster for app: ${register.appId}, $register")
      context.watch(appMaster)
      appMasterRegistry += register.appId -> (appMaster, register)
      kvService ! PutKV(MASTER_GROUP, MASTER_STATE,
        MasterState(nextAppId, appMasterRegistry, activeAppMasters, deadAppMasters))
      sender ! AppMasterRegistered(register.appId)

    case ActivateAppMaster(appId) =>
      LOG.info(s"Activate AppMaster for app $appId")
      activeAppMasters += appId
      kvService ! PutKV(MASTER_GROUP, MASTER_STATE,
        MasterState(this.nextAppId, appMasterRegistry, activeAppMasters, deadAppMasters))
      sender ! AppMasterActivated(appId)
  }

  def appDataStoreService: Receive = {
    case SaveAppData(appId, key, value) =>
      val client = sender()
      (kvService ? PutKV(appId.toString, key, value)).asInstanceOf[Future[PutKVResult]].map {
        case PutKVSuccess =>
          client ! AppDataSaved
        case PutKVFailed(k, ex) =>
          client ! SaveAppDataFailed
      }
    case GetAppData(appId, key) =>
      val client = sender()
      (kvService ? GetKV(appId.toString, key)).asInstanceOf[Future[GetKVResult]].map {
        case GetKVSuccess(privateKey, value) =>
          client ! GetAppDataResult(key, value)
        case GetKVFailed(ex) =>
          client ! GetAppDataResult(key, null)
      }
  }

  def terminationWatch: Receive = {
    case terminate: Terminated =>
      LOG.info(s"AppMaster(${terminate.actor.path}) is terminated, " +
        s"network down: ${terminate.getAddressTerminated}")

      // Now we assume that the only normal way to stop the application is submitting a
      // ShutdownApplication request
      val application = appMasterRegistry.find { appInfo =>
        val (_, (actorRef, _)) = appInfo
        actorRef.compareTo(terminate.actor) == 0
      }
      if (application.nonEmpty) {
        val appId = application.get._1
        (kvService ? GetKV(appId.toString, APP_STATE)).asInstanceOf[Future[GetKVResult]].map {
          case GetKVSuccess(_, result) =>
            val appState = result.asInstanceOf[ApplicationState]
            if (appState != null) {
              LOG.info(s"Recovering application, $appId")
              self ! RecoverApplication(appState)
            } else {
              LOG.error(s"Cannot find application state for $appId")
            }
          case GetKVFailed(ex) =>
            LOG.error(s"Cannot find master state to recover")
        }
      }
  }

  def selfMsgHandler: Receive = {
    case RecoverApplication(state) =>
      val appId = state.appId
      if (appMasterRestartPolicies.get(appId).get.allowRestart) {
        LOG.info(s"AppManager Recovering Application $appId...")
        activeAppMasters -= appId
        kvService ! PutKV(MASTER_GROUP, MASTER_STATE,
          MasterState(this.nextAppId, appMasterRegistry, activeAppMasters, deadAppMasters))
        context.actorOf(launcher.props(appId, EXECUTOR_ID, state.app, state.jar, state.username,
          context.parent, None), s"launcher${appId}_${Util.randInt()}")
      } else {
        LOG.error(s"Application $appId failed too many times")
      }
  }

  case class RecoverApplication(applicationStatus: ApplicationState)

  private def cleanApplicationData(appId: Int): Unit = {
    if (appMasterRegistry.contains(appId)) {
      // Add the dead app to dead appMasters
      deadAppMasters += appId
      // Remove the dead app from active appMasters
      activeAppMasters -= appId

      appMasterRegistry += appId -> {
        val (ref, info) = appMasterRegistry(appId)
        (ref, info.copy(finishTime = System.currentTimeMillis()))
      }
      kvService ! PutKV(MASTER_GROUP, MASTER_STATE,
        MasterState(this.nextAppId, appMasterRegistry, activeAppMasters, deadAppMasters))
      kvService ! DeleteKVGroup(appId.toString)
    }
  }

  private def applicationNameExist(appName: String): Boolean = {
    appMasterRegistry.values.exists { case (_, info) =>
      info.appName == appName && !deadAppMasters.contains(info.appId)
    }
  }
}

object AppManager {
  final val APP_STATE = "app_state"
  // The id is used in KVStore
  final val MASTER_STATE = "master_state"

  case class MasterState(
      maxId: Int,
      appMasterRegistry: Map[Int, (ActorRef, AppMasterRuntimeInfo)],
      activeAppMasters: Set[Int],
      deadAppMasters: Set[Int])
}
