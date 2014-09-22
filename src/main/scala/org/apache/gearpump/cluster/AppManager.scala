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
import akka.contrib.datareplication.{GSet, DataReplication}
import akka.contrib.datareplication.Replicator._
import org.apache.gearpump.cluster.AppMasterToMaster._
import org.apache.gearpump.cluster.AppMasterToWorker._
import org.apache.gearpump.cluster.ClientToMaster._
import org.apache.gearpump.cluster.MasterToAppMaster._
import org.apache.gearpump.cluster.MasterToClient.{ShutdownApplicationResult, SubmitApplicationResult}
import org.apache.gearpump.cluster.WorkerToAppMaster._
import org.apache.gearpump.scheduler.{ResourceRequest, Allocation, Resource}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.ActorSystemBooter.{ActorCreated, BindLifeCycle, CreateActor, RegisterActorSystem}
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util._
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, Props, _}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.contrib.datareplication.Replicator._
import akka.contrib.pattern.ClusterSingletonManager
import com.typesafe.config.ConfigFactory

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration._

import scala.concurrent.duration._
import akka.actor.ActorLogging
import akka.cluster.Cluster
import akka.actor.Actor
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Props
import akka.contrib.datareplication.{GSet, DataReplication}
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
/**
 * AppManager is dedicated part of Master to manager applicaitons
 */

class ApplicationState(val appId : Int, val attemptId : Int, val state : Any) extends Serializable {

  override def equals(other: Any): Boolean = {
    if (other.isInstanceOf[ApplicationState]) {
      val that = other.asInstanceOf[ApplicationState]
      if (appId == that.appId && attemptId == that.attemptId) {
        return true
      } else {
        return false
      }
    } else {
      return false
    }
  }

  override def hashCode: Int = {
    import akka.routing.MurmurHash._
    extendHash(appId, attemptId, startMagicA, startMagicB)
  }
}

private[cluster] class AppManager() extends Actor with Stash {
  import org.apache.gearpump.cluster.AppManager._

  private var master : ActorRef = null
  private var executorCount : Int = 0;
  private var appId : Int = 0;

  private val systemconfig = context.system.settings.config

  def receive : Receive = null

  //from appid to appMaster data
  private var appMasterRegistry = Map.empty[Int, AppMasterInfo]

  private val STATE = "masterstate"
  private val TIMEOUT = Duration(5, TimeUnit.SECONDS)
  private val replicator = DataReplication(context.system).replicator

  //TODO: We can use this state for appmaster HA to recover a new App master
  private var state : Set[ApplicationState] = Set.empty[ApplicationState]

  val masterClusterSize = systemconfig.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).size()

  //optimize write path, we can tollerate one master down for recovery.
  val writeQuorum = Math.min(2, masterClusterSize / 2 + 1)
  val readQuorum = masterClusterSize + 1 - writeQuorum

  replicator ! new Get(STATE, ReadFrom(readQuorum), TIMEOUT, None)

  override def preStart : Unit = {
    replicator ! Subscribe(STATE, self)
  }

  override def postStop : Unit = {
    replicator ! Unsubscribe(STATE, self)
  }

  LOG.info("Recoving application state....")
  context.become(waitForMasterState)

  def waitForMasterState : Receive = {
    case GetSuccess(_, replicatedState : GSet, _) =>
      state = replicatedState.getValue().asScala.foldLeft(state) { (set, appState) =>
        set + appState.asInstanceOf[ApplicationState]
      }
      appId = state.map(_.appId).size
      LOG.info(s"Successfully recoeved application states for ${state.map(_.appId)}, nextAppId: ${appId}....")
      context.become(receiveHandler)
      unstashAll()
    case x : GetFailure =>
      LOG.info("GetFailure We cannot find any exisitng state, start a fresh one...")
      context.become(receiveHandler)
      unstashAll()
    case x : NotFound =>
      LOG.info("We cannot find any exisitng state, start a fresh one...")
      context.become(receiveHandler)
      unstashAll()
    case msg =>
      LOG.info(s"Get information ${msg.getClass.getSimpleName}")
      stash()
  }

  def receiveHandler = masterHAMsgHandler orElse clientMsgHandler orElse appMasterMessage orElse terminationWatch

  def masterHAMsgHandler : Receive = {
    case update: UpdateResponse => LOG.info(s"we get update $update")
    case Changed(STATE, data: GSet) =>
      LOG.info("Current elements: {}", data.value)
  }

  def clientMsgHandler : Receive = {
    case submitApp @ SubmitApplication(appMasterClass, config, app) =>
      LOG.info(s"AppManager Submiting Application $appId...")
      val appWatcher = context.actorOf(Props(classOf[AppMasterStarter], appId, appMasterClass, config, app), appId.toString)

      LOG.info(s"Persist master state writeQuorum: ${writeQuorum}, timeout: ${TIMEOUT}...")
      replicator ! Update(STATE, GSet(), WriteTo(writeQuorum), TIMEOUT)(_ + new ApplicationState(appId, 0, null))
      sender.tell(SubmitApplicationResult(Success(appId)), context.parent)
      appId += 1
    case ShutdownApplication(appId) =>
      LOG.info(s"App Manager Shutting down application $appId")
      val data = appMasterRegistry.get(appId)
      if (data.isDefined) {
        val worker = data.get.worker
        LOG.info(s"Shuttdown app master at ${worker.path.toString}, appId: $appId, executorId: $masterExecutorId")
        worker ! ShutdownExecutor(appId, masterExecutorId, s"AppMaster $appId shutdown requested by master...")
        sender ! ShutdownApplicationResult(Success(appId))
      }
      else {
        val errorMsg = s"Find to find regisration information for appId: $appId"
        LOG.error(errorMsg)
        sender ! ShutdownApplicationResult(Failure(new Exception(errorMsg)))
      }
  }

  def appMasterMessage : Receive = {
    case RegisterAppMaster(appMaster, appId, executorId, slots, registerData : AppMasterInfo) =>
      LOG.info(s"Register AppMaster for app: $appId...")
      context.watch(appMaster)
      appMasterRegistry += appId -> registerData
      sender ! AppMasterRegistered(appId, context.parent)
  }

  def terminationWatch : Receive = {
    //TODO: fix this
    case terminate : Terminated => {
      terminate.getAddressTerminated()
      //TODO: Check whether this belongs to a app master
      LOG.info(s"App Master is terminiated, network down: ${terminate.getAddressTerminated()}")

      //TODO: decide whether it is a normal terminaiton, or abnormal. and implement appMaster HA
    }
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
    master ! RequestResource(appId, ResourceRequest(1, null))
    LOG.info(s"AppManager asking Master for resource for app $appId...")

    def receive : Receive = waitForResourceAllocation

    def waitForResourceAllocation : Receive = {
      case ResourceAllocated(allocations) => {
        LOG.info(s"Resource allocated for appMaster $app Id")
        val allocation = allocations(0)
        val appMasterConfig = appConfig.withAppId(appId).withAppDescription(app).withAppMasterRegisterData(AppMasterInfo(allocation.worker)).withExecutorId(masterExecutorId).withSlots(allocation)
        //LOG.info(s"Try to launch a executor for app Master on $worker for app $appId")
        val name = actorNameForExecutor(appId, masterExecutorId)
        val selfPath = ActorUtil.getFullPath(context)

        val executionContext = ExecutorContext(Util.getCurrentClassPath, context.system.settings.config.getString(Constants.GEARPUMP_APPMASTER_ARGS).split(" "), classOf[ActorSystemBooter].getName, Array(name, selfPath))

        allocation.worker ! LaunchExecutor(appId, masterExecutorId, allocation, executionContext)
        context.become(waitForActorSystemToStart(allocation.worker, appMasterConfig))
      }
    }

    def waitForActorSystemToStart(worker : ActorRef, masterConfig : Configs) : Receive = {
      case ExecutorLaunchRejected(reason, ex) =>
        LOG.error(s"Executor Launch failed reason：$reason", ex)
        //TODO: ask master to allocate new resources and start appmaster on new node.
        context.stop(self)
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