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

import java.util.concurrent.{TimeUnit, TimeoutException}

import akka.actor._
import akka.cluster.Cluster
import akka.pattern.ask
import org.apache.gearpump.cluster.AppMasterToMaster._
import org.apache.gearpump.cluster.AppMasterToWorker._
import org.apache.gearpump.cluster.ClientToMaster._
import InMemoryKVService._
import MasterHAService._
import org.apache.gearpump.cluster.MasterToAppMaster._
import org.apache.gearpump.cluster.MasterToClient.{ReplayApplicationResult, ResolveAppIdResult, ShutdownApplicationResult, SubmitApplicationResult}
import org.apache.gearpump.cluster.WorkerToAppMaster._
import org.apache.gearpump.cluster._
import org.apache.gearpump.cluster.scheduler.{Resource, ResourceAllocation, ResourceRequest}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.ActorSystemBooter._
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util._
import org.slf4j.Logger

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
 * AppManager is dedicated part of Master to manager applicaitons
 */
private[cluster] class AppManager(masterHA : ActorRef, kvService: ActorRef, launcher: AppMasterLauncherFactory) extends Actor with Stash with TimeOutScheduler{
  private val LOG: Logger = LogUtil.getLogger(getClass)

  private val executorId : Int = APPMASTER_DEFAULT_EXECUTOR_ID
  private val systemconfig = context.system.settings.config

  implicit val timeout = FUTURE_TIMEOUT
  implicit val executionContext = context.dispatcher

  //next available appId
  private var appId: Int = 0

  //from appid to appMaster data
  private var appMasterRegistry = Map.empty[Int, (ActorRef, AppMasterRuntimeInfo)]

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
      val appLauncher = context.actorOf(launcher.props(appId, executorId, app, jar, username, context.parent, Some(client)), s"launcher${appId}_${Util.randInt}")

      val appState = new ApplicationState(appId, 0, app, jar, username, null)
      masterHA ! UpdateMasterState(appState)
      appId += 1

    case ShutdownApplication(appId) =>
      LOG.info(s"App Manager Shutting down application $appId")
      val (appMaster, info) = appMasterRegistry.getOrElse(appId, (null, null))
      Option(info) match {
        case Some(info) =>
          val worker = info.worker
          LOG.info(s"Shuttdown app master at ${worker.path}, appId: $appId, executorId: $executorId")
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
      val appMastersData = collection.mutable.ListBuffer[AppMasterData]()
      appMasterRegistry.foreach(pair => {
        val (id, (appMaster:ActorRef, info:AppMasterRuntimeInfo)) = pair
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
    case RegisterAppMaster(appMaster, register: AppMasterRuntimeInfo) =>
      val appMasterPath = appMaster.path.address.toString
      val workerPath = register.worker.path.address.toString
      LOG.info(s"Register AppMaster for app: ${register.appId} appMaster=$appMasterPath worker=$workerPath")
      context.watch(appMaster)
      appMasterRegistry += register.appId -> (appMaster, register)
      sender ! AppMasterRegistered(register.appId, context.parent)
  }

  def appDataStoreService: Receive = {
    case SaveAppData(appId, key, value) =>
      val client = sender
      (kvService ? PutKV(appId.toString, key, value)).asInstanceOf[Future[PutKVResult]].map { result =>
        result match {
          case PutKVSuccess =>
            client ! AppDataSaved
          case PutKVFailed(ex) =>
            client ! SaveAppDataFailed
        }
      }
    case GetAppData(appId, key) =>
      val client = sender

      (kvService ? GetKV(appId.toString, key)).asInstanceOf[Future[GetKVResult]].map {result =>
        result match {
          case GetKVSuccess(privateKey, value) =>
            client ! GetAppDataResult(key, value)
          case GetKVFailed(ex) =>
            client ! GetAppDataResult(key, null)
        }
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
        (masterHA ? GetMasterState).asInstanceOf[Future[GetMasterStateResult]].map { masterState =>
          masterState match {
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
  }

  def selfMsgHandler: Receive = {
    case RecoverApplication(state) =>
      val appId = state.appId
      LOG.info(s"AppManager Recovering Application $appId...")
      context.actorOf(launcher.props(appId, executorId, state.app, state.jar, state.username, context.parent, None), s"launcher${appId}_${Util.randInt}")
  }

  case class RecoverApplication(applicationStatus : ApplicationState)

  private def cleanApplicationData(appId : Int) : Unit = {
    appMasterRegistry -= appId
    masterHA ! DeleteMasterState(appId)
    kvService ! DeleteKVGroup(appId.toString)
  }
}

case class AppMasterRuntimeInfo(worker : ActorRef, appId: Int = 0, resource: Resource = Resource.empty) extends AppMasterRegisterData