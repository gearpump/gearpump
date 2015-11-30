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

package io.gearpump.cluster.master

import akka.actor._
import akka.pattern.ask
import io.gearpump.cluster.AppMasterToMaster.{AppDataSaved, SaveAppDataFailed, _}
import io.gearpump.cluster.AppMasterToWorker._
import io.gearpump.cluster.ClientToMaster._
import io.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMasterDataRequest, AppMastersDataRequest, _}
import io.gearpump.cluster.MasterToClient._
import io.gearpump.cluster.WorkerToAppMaster.{ShutdownExecutorFailed, _}
import io.gearpump.cluster.appmaster.{AppMasterRuntimeInfo, ApplicationState}
import io.gearpump.cluster.master.AppManager._
import io.gearpump.cluster.master.InMemoryKVService.{GetKVResult, PutKVResult, PutKVSuccess, _}
import io.gearpump.cluster.master.Master._
import io.gearpump.util.Constants._
import io.gearpump.util.{ActorUtil, TimeOutScheduler, Util, _}
import org.slf4j.Logger

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * AppManager is dedicated part of Master to manager applicaitons
 */
private[cluster] class AppManager(kvService: ActorRef, launcher: AppMasterLauncherFactory) extends Actor with Stash with TimeOutScheduler{
  private val LOG: Logger = LogUtil.getLogger(getClass)

  private val executorId : Int = APPMASTER_DEFAULT_EXECUTOR_ID
  private val appMasterMaxRetries: Int = 5
  private val appMasterRetryTimeRange: Duration = 20 seconds

  implicit val timeout = FUTURE_TIMEOUT
  implicit val executionContext = context.dispatcher

  //next available appId
  private var appId: Int = 1

  //from appid to appMaster data
  private var appMasterRegistry = Map.empty[Int, (ActorRef, AppMasterRuntimeInfo)]

  // dead appmaster list
  private var deadAppMasters = Map.empty[Int, (ActorRef, AppMasterRuntimeInfo)]

  private var appMasterRestartPolicies = Map.empty[Int, RestartPolicy]

  def receive: Receive = null

  kvService ! GetKV(MASTER_GROUP, MASTER_STATE)
  context.become(waitForMasterState)

  def waitForMasterState: Receive = {
    case GetKVSuccess(_, result) =>
      val masterState = result.asInstanceOf[MasterState]
      if(masterState != null) {
        this.appId = masterState.maxId + 1
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

  def receiveHandler : Receive = {
    val msg = "Application Manager started. Ready for application submission..."
    System.out.println(msg)
    LOG.info(msg)
    clientMsgHandler orElse appMasterMessage orElse selfMsgHandler orElse workerMessage orElse appDataStoreService orElse terminationWatch
  }

  def clientMsgHandler: Receive = {
    case SubmitApplication(app, jar, username) =>
      LOG.info(s"Submit Application ${app.name}($appId) by $username...")
      val client = sender
      if (applicationNameExist(app.name)) {
        client ! SubmitApplicationResult(Failure(new Exception(s"Application name ${app.name} already existed")))
      } else {
        context.actorOf(launcher.props(appId, executorId, app, jar, username, context.parent, Some(client)), s"launcher${appId}_${Util.randInt}")

        val appState = new ApplicationState(appId, app.name, 0, app, jar, username, null)
        appMasterRestartPolicies += appId -> new RestartPolicy(appMasterMaxRetries, appMasterRetryTimeRange)
        kvService ! PutKV(appId.toString, APP_STATE, appState)
        appId += 1
      }

    case RestartApplication(appId) =>
      (kvService ? GetKV(appId.toString, APP_STATE)).asInstanceOf[Future[GetKVResult]].map {
        case GetKVSuccess(_, result) =>
          val appState = result.asInstanceOf[ApplicationState]
          if (appState != null) {
            LOG.info(s"Shutting down the application (restart), $appId")
            self ! ShutdownApplication(appId)
            self forward SubmitApplication(appState.app, appState.jar, appState.username)
          } else {
            sender ! SubmitApplicationResult(Failure(
              new Exception(s"Failed to restart, because the application $appId does not exist.")
            ))
          }
        case GetKVFailed(ex) =>
          sender ! SubmitApplicationResult(Failure(
            new Exception(s"Unable to obtain the Master State. Application $appId will not be restarted.")
          ))
      }

    case ShutdownApplication(appId) =>
      LOG.info(s"App Manager Shutting down application $appId")
      val (_, info) = appMasterRegistry.getOrElse(appId, (null, null))
      Option(info) match {
        case Some(info) =>
          val worker = info.worker
          LOG.info(s"Shutdown AppMaster at ${Option(worker).map(_.path).orNull}, appId: $appId, executorId: $executorId")
          cleanApplicationData(appId)
          val shutdown = ShutdownExecutor(appId, executorId, s"AppMaster $appId shutdown requested by master...")
          sendMsgWithTimeOutCallBack(worker, shutdown, 30000, shutDownExecutorTimeOut())
          sender ! ShutdownApplicationResult(Success(appId))
        case None =>
          val errorMsg = s"Failed to find regisration information for appId: $appId"
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
          val (_, info) = appMasterRegistry(appId)
          info.config
        } else if (deadAppMasters.contains(appId)) {
          val (_, info) = deadAppMasters(appId)
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
          val appMasterPath = ActorUtil.getFullPath(context.system, appMaster.path)
          val workerPath = Option(info.worker).map(
            worker => ActorUtil.getFullPath(context.system, worker.path)).orNull
          sender ! AppMasterData(
            appStatus, appId, info.appName, appMasterPath, workerPath,
            info.submissionTime, info.startTime, info.finishTime, info.user)

        case AppMasterNonExist =>
          sender ! AppMasterData(AppMasterNonExist)
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
      kvService ! PutKV(MASTER_GROUP, MASTER_STATE, MasterState(appId, appMasterRegistry, deadAppMasters))
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
      LOG.info(s"AppMaster(${terminate.actor.path}) is terminiated, network down: ${terminate.getAddressTerminated()}")
      //Now we assume that the only normal way to stop the application is submitting a ShutdownApplication request
      val application = appMasterRegistry.find{appInfo =>
        val (_, (actorRef, _)) = appInfo
        actorRef.compareTo(terminate.actor) == 0
      }
      if(application.nonEmpty){
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
      if(appMasterRestartPolicies.get(appId).get.allowRestart) {
        LOG.info(s"AppManager Recovering Application $appId...")
        context.actorOf(launcher.props(appId, executorId, state.app, state.jar, state.username, context.parent, None), s"launcher${appId}_${Util.randInt}")
      } else {
        LOG.error(s"Application $appId failed too many times")
      }
  }

  case class RecoverApplication(applicationStatus : ApplicationState)

  private def cleanApplicationData(appId : Int) : Unit = {
    //add the dead app to dead appMaster
    appMasterRegistry.get(appId).foreach { pair =>
      val (appMasterActor, info) = pair
      deadAppMasters += appId -> (appMasterActor, info.copy(finishTime = System.currentTimeMillis()))
    }

    appMasterRegistry -= appId

    kvService ! PutKV(MASTER_GROUP, MASTER_STATE, MasterState(this.appId, appMasterRegistry, deadAppMasters))
    kvService ! DeleteKVGroup(appId.toString)
  }

  private def applicationNameExist(appName: String): Boolean = {
    appMasterRegistry.values.exists(_._2.appName == appName)
  }
}

object AppManager {
  final val APP_STATE = "app_state"
  //The id is used in KVStore
  final val MASTER_STATE = "master_state"

  case class MasterState(
      maxId: Int,
      appMasterRegistry: Map[Int, (ActorRef, AppMasterRuntimeInfo)],
      deadAppMasters: Map[Int, (ActorRef, AppMasterRuntimeInfo)])
}
