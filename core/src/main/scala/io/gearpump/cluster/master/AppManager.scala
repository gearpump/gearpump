/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.cluster.master

import akka.actor._
import akka.pattern.ask
import com.typesafe.config.{Config, ConfigFactory}
import io.gearpump.Time.MilliSeconds
import io.gearpump.cluster.{ApplicationStatus, ApplicationTerminalStatus}
import io.gearpump.cluster.AppMasterToMaster.{AppDataSaved, SaveAppDataFailed, _}
import io.gearpump.cluster.ClientToMaster._
import io.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMasterDataRequest, AppMastersDataRequest, _}
import io.gearpump.cluster.MasterToClient._
import io.gearpump.cluster.WorkerToAppMaster.{ShutdownExecutorFailed, _}
import io.gearpump.cluster.appmaster.{ApplicationMetaData, ApplicationRuntimeInfo}
import io.gearpump.cluster.master.AppManager._
import io.gearpump.cluster.master.InMemoryKVService.{GetKVResult, PutKVResult, PutKVSuccess, _}
import io.gearpump.cluster.master.Master._
import io.gearpump.util.{ActorUtil, Constants, LogUtil, RestartPolicy, TimeOutScheduler, Util}
import io.gearpump.util.Constants._
import org.slf4j.Logger
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * AppManager is dedicated child of Master to manager all applications.
 */
private[cluster] class AppManager(kvService: ActorRef, launcher: AppMasterLauncherFactory)
  extends Actor with Stash with TimeOutScheduler {

  private val LOG: Logger = LogUtil.getLogger(getClass)
  private val systemConfig: Config = context.system.settings.config

  private val appTotalRetries: Int = systemConfig.getInt(Constants.APPLICATION_TOTAL_RETRIES)

  implicit val timeout = FUTURE_TIMEOUT
  implicit val executionContext = context.dispatcher

  // Next available appId
  private var nextAppId: Int = 1

  private var applicationRegistry = Map.empty[Int, ApplicationRuntimeInfo]
  private var applicationResults = Map.empty[Int, ApplicationResult]
  private var appResultListeners = Map.empty[Int, List[ActorRef]]

  private var appMasterRestartPolicies = Map.empty[Int, RestartPolicy]

  def receive: Receive = null

  kvService ! GetKV(MASTER_GROUP, MASTER_STATE)
  context.become(waitForMasterState)

  def waitForMasterState: Receive = {
    case GetKVSuccess(_, result) =>
      val masterState = result.asInstanceOf[MasterState]
      if (masterState != null) {
        this.nextAppId = masterState.maxId + 1
        this.applicationRegistry = masterState.applicationRegistry
      }
      context.become(receiveHandler)
      unstashAll()
    case GetKVFailed(ex) =>
      LOG.error("Failed to get master state, shutting down master to avoid data corruption...", ex)
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
        context.actorOf(launcher.props(nextAppId, APPMASTER_DEFAULT_EXECUTOR_ID, app, jar, username,
          context.parent, Some(client)), s"launcher${nextAppId}_${Util.randInt()}")
        appMasterRestartPolicies += nextAppId -> new RestartPolicy(appTotalRetries)

        val appRuntimeInfo = ApplicationRuntimeInfo(nextAppId, app.name,
          user = username,
          submissionTime = System.currentTimeMillis(),
          config = app.clusterConfig,
          status = ApplicationStatus.PENDING)
        applicationRegistry += nextAppId -> appRuntimeInfo
        val appMetaData = ApplicationMetaData(nextAppId, 0, app, jar, username)
        kvService ! PutKV(nextAppId.toString, APP_METADATA, appMetaData)

        nextAppId += 1
        kvService ! PutKV(MASTER_GROUP, MASTER_STATE, MasterState(nextAppId, applicationRegistry))
      }

    case RestartApplication(appId) =>
      val client = sender()
      (kvService ? GetKV(appId.toString, APP_METADATA)).asInstanceOf[Future[GetKVResult]].foreach {
        case GetKVSuccess(_, result) =>
          val metaData = result.asInstanceOf[ApplicationMetaData]
          if (metaData != null) {
            LOG.info(s"Shutting down the application (restart), $appId")
            self ! ShutdownApplication(appId)
            self.tell(SubmitApplication(metaData.appDesc, metaData.jar, metaData.username), client)
          } else {
            client ! SubmitApplicationResult(Failure(
              new Exception(s"Failed to restart, because the application $appId does not exist.")
            ))
          }
        case GetKVFailed(_) =>
          client ! SubmitApplicationResult(Failure(
            new Exception(s"Unable to obtain the Master State. " +
              s"Application $appId will not be restarted.")
          ))
      }

    case ShutdownApplication(appId) =>
      LOG.info(s"App Manager Shutting down application $appId")
      val appInfo = applicationRegistry.get(appId).
        filter(!_.status.isInstanceOf[ApplicationTerminalStatus])
      appInfo match {
        case Some(info) =>
          shutdownApplication(info)
          sender ! ShutdownApplicationResult(Success(appId))
          // Here we use the function to make sure the status is consistent because
          // sending another message to self will involve timing problem
          this.onApplicationStatusChanged(appId, ApplicationStatus.TERMINATED,
            System.currentTimeMillis())
        case None =>
          val errorMsg = s"Failed to find registration information for appId: $appId"
          LOG.error(errorMsg)
          sender ! ShutdownApplicationResult(Failure(new Exception(errorMsg)))
      }

    case ResolveAppId(appId) =>
      val appMaster = applicationRegistry.get(appId).map(_.appMaster)
      appMaster match {
        case Some(appMasterActor) =>
          sender ! ResolveAppIdResult(Success(appMasterActor))
        case None =>
          sender ! ResolveAppIdResult(Failure(new Exception(s"Can not find Application: $appId")))
      }

    case AppMastersDataRequest =>
      val appMastersData = collection.mutable.ListBuffer[AppMasterData]()
      applicationRegistry.foreach(pair => {
        val (id, info: ApplicationRuntimeInfo) = pair
        val appMasterPath = ActorUtil.getFullPath(context.system, info.appMaster)
        val workerPath = Option(info.worker).map(worker =>
          ActorUtil.getFullPath(context.system, worker))
        appMastersData += AppMasterData(
          info.status, id, info.appName, appMasterPath, workerPath.orNull,
          info.submissionTime, info.startTime, info.finishTime, info.user)
      })
      sender ! AppMastersData(appMastersData.toList)

    case QueryAppMasterConfig(appId) =>
      val config = applicationRegistry.get(appId).map(_.config).getOrElse(ConfigFactory.empty())
      sender ! AppMasterConfig(config)

    case appMasterDataRequest: AppMasterDataRequest =>
      val appId = appMasterDataRequest.appId
      val appRuntimeInfo = applicationRegistry.get(appId)
      appRuntimeInfo match {
        case Some(info) =>
          val appMasterPath = ActorUtil.getFullPath(context.system, info.appMaster.path)
          val workerPath = Option(info.worker).map(
            worker => ActorUtil.getFullPath(context.system, worker.path)).orNull
          sender ! AppMasterData(
            info.status, appId, info.appName, appMasterPath, workerPath,
            info.submissionTime, info.startTime, info.finishTime, info.user)
        case None =>
          sender ! AppMasterData(ApplicationStatus.NONEXIST)
      }

    case RegisterAppResultListener(appId) =>
      val listenerList = appResultListeners.getOrElse(appId, List.empty[ActorRef])
      appResultListeners += appId -> (listenerList :+ sender())
  }

  def workerMessage: Receive = {
    case ShutdownExecutorSucceed(appId, executorId) =>
      LOG.info(s"Shut down executor $executorId for application $appId successfully")
    case failed: ShutdownExecutorFailed =>
      LOG.error(failed.reason)
  }

  def appMasterMessage: Receive = {
    case RegisterAppMaster(appId, appMaster, workerInfo) =>
      val appInfo = applicationRegistry.get(appId)
      appInfo match {
        case Some(info) =>
          LOG.info(s"Register AppMaster for app: $appId")
          val updatedInfo = info.onAppMasterRegistered(appMaster, workerInfo.ref)
          context.watch(appMaster)
          applicationRegistry += appId -> updatedInfo
          kvService ! PutKV(MASTER_GROUP, MASTER_STATE, MasterState(nextAppId, applicationRegistry))
          sender ! AppMasterRegistered(appId)
        case None =>
          LOG.error(s"Can not find submitted application $appId")
      }

    case ApplicationStatusChanged(appId, newStatus, timeStamp) =>
      onApplicationStatusChanged(appId, newStatus, timeStamp)
  }

  private def onApplicationStatusChanged(appId: Int, newStatus: ApplicationStatus,
      timeStamp: MilliSeconds): Unit = {
    applicationRegistry.get(appId) match {
      case Some(appRuntimeInfo) =>
        if (appRuntimeInfo.status.canTransitTo(newStatus)) {
          var updatedStatus: ApplicationRuntimeInfo = null
          LOG.info(s"Application $appId change to ${newStatus.toString} at $timeStamp")
          newStatus match {
            case ApplicationStatus.ACTIVE =>
              updatedStatus = appRuntimeInfo.onAppMasterActivated(timeStamp)
              sender ! AppMasterActivated(appId)
            case status: ApplicationTerminalStatus =>
              shutdownApplication(appRuntimeInfo)
              updatedStatus = appRuntimeInfo.onFinalStatus(timeStamp, status)
              applicationResults += appId -> ApplicationStatus.toResult(status, appId)
            case status =>
              LOG.error(s"App $appId should not change it's status to $status")
          }

          if (newStatus.isInstanceOf[ApplicationTerminalStatus]) {
            kvService ! DeleteKVGroup(appId.toString)
          }
          applicationRegistry += appId -> updatedStatus
          kvService ! PutKV(MASTER_GROUP, MASTER_STATE, MasterState(nextAppId, applicationRegistry))
        } else {
          LOG.error(s"Application $appId tries to switch status ${appRuntimeInfo.status} " +
            s"to $newStatus")
        }
      case None =>
        LOG.error(s"Can not find application runtime info for appId $appId when it's " +
          s"status changed to ${newStatus.toString}")
    }
  }

  private def sendAppResultToListeners(appId: Int, result: ApplicationResult): Unit = {
    appResultListeners.get(appId).foreach {
      _.foreach { client =>
        client ! result
      }
    }
  }

  def appDataStoreService: Receive = {
    case SaveAppData(appId, key, value) =>
      val client = sender()
      (kvService ? PutKV(appId.toString, key, value)).asInstanceOf[Future[PutKVResult]].map {
        case PutKVSuccess =>
          client ! AppDataSaved
        case PutKVFailed(_, _) =>
          client ! SaveAppDataFailed
      }
    case GetAppData(appId, key) =>
      val client = sender()
      (kvService ? GetKV(appId.toString, key)).asInstanceOf[Future[GetKVResult]].map {
        case GetKVSuccess(_, value) =>
          client ! GetAppDataResult(key, value)
        case GetKVFailed(_) =>
          client ! GetAppDataResult(key, null)
      }
  }

  def terminationWatch: Receive = {
    case terminate: Terminated =>
      LOG.info(s"AppMaster(${terminate.actor.path}) is terminated, " +
        s"network down: ${terminate.getAddressTerminated}")

      // Now we assume that the only normal way to stop the application is submitting a
      // ShutdownApplication request
      applicationRegistry.find(_._2.appMaster.equals(terminate.actor)).foreach {
        case (appId, info) =>
          info.status match {
            case _: ApplicationTerminalStatus =>
              sendAppResultToListeners(appId, applicationResults(appId))
            case _ =>
              (kvService ? GetKV(appId.toString, APP_METADATA))
                .asInstanceOf[Future[GetKVResult]].map {
                case GetKVSuccess(_, result) =>
                  val appMetadata = result.asInstanceOf[ApplicationMetaData]
                  if (appMetadata != null) {
                    LOG.info(s"Recovering application, $appId")
                    val updatedInfo = info.copy(status = ApplicationStatus.PENDING)
                    applicationRegistry += appId -> updatedInfo
                    self ! RecoverApplication(appMetadata)
                  } else {
                    LOG.error(s"Cannot find application meta data for $appId")
                  }
                case GetKVFailed(ex) =>
                  LOG.error(s"Cannot find master state to recover", ex)
              }
          }
      }
  }

  def selfMsgHandler: Receive = {
    case RecoverApplication(state) =>
      val appId = state.appId
      if (appMasterRestartPolicies(appId).allowRestart) {
        LOG.info(s"AppManager Recovering Application $appId...")
        kvService ! PutKV(MASTER_GROUP, MASTER_STATE,
          MasterState(this.nextAppId, applicationRegistry))
        context.actorOf(launcher.props(appId, APPMASTER_DEFAULT_EXECUTOR_ID, state.appDesc,
          state.jar, state.username, context.parent, None), s"launcher${appId}_${Util.randInt()}")
      } else {
        LOG.error(s"Application $appId failed too many times")
      }
  }

  private def shutdownApplication(info: ApplicationRuntimeInfo): Unit = {
    info.appMaster ! ShutdownApplication(info.appId)
  }

  private def applicationNameExist(appName: String): Boolean = {
    applicationRegistry.values.exists { info =>
      info.appName == appName && !info.status.isInstanceOf[ApplicationTerminalStatus]
    }
  }
}

object AppManager {
  final val APP_METADATA = "app_metadata"
  // The id is used in KVStore
  final val MASTER_STATE = "master_state"

  case class RecoverApplication(appMetaData: ApplicationMetaData)

  case class MasterState(maxId: Int, applicationRegistry: Map[Int, ApplicationRuntimeInfo])
}
