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

package org.apache.gearpump.cluster.appmaster

import akka.actor._

import org.apache.gearpump.cluster.AppMasterToMaster.RegisterAppMaster
import org.apache.gearpump.cluster.appmaster.AppMasterRuntimeEnvironment._
import org.apache.gearpump.cluster.appmaster.ExecutorSystemScheduler.{Session, StartExecutorSystems}
import org.apache.gearpump.cluster.appmaster.MasterConnectionKeeper.MasterConnectionStatus._
import org.apache.gearpump.cluster.master.MasterProxy
import org.apache.gearpump.cluster.{AppDescription, AppMasterContext}
import org.apache.gearpump.util.LogUtil

import scala.concurrent.duration._

/**
 * This serves as runtime environment for AppMaster.
 * When starting an AppMaster, we need to setup the connection to master,
 * and prepare other environments.
 *
 * This also extend the function of Master, by providing a scheduler service for Executor System.
 * AppMaster can ask Master for executor system directly. details like requesting resource,
 * contacting worker to start a process, and then starting an executor system is hidden from
 * AppMaster.
 *
 * Please use AppMasterRuntimeEnvironment.props() to construct this actor.
 */
private[appmaster]
class AppMasterRuntimeEnvironment(
    appContextInput: AppMasterContext,
    app: AppDescription,
    masters: Iterable[ActorPath],
    masterFactory: (AppId, MasterActorRef) => Props,
    appMasterFactory: (AppMasterContext, AppDescription) => Props,
    masterConnectionKeeperFactory: (MasterActorRef, RegisterAppMaster, ListenerActorRef) => Props)
  extends Actor {

  private val appId = appContextInput.appId
  private val LOG = LogUtil.getLogger(getClass, app = appId)

  private val master = context.actorOf(
    masterFactory(appId, context.actorOf(Props(new MasterProxy(masters, 30.seconds)))))
  private val appContext = appContextInput.copy(masterProxy = master)

  // Create appMaster proxy to receive command and forward to appmaster
  private val appMaster = context.actorOf(appMasterFactory(appContext, app))
  context.watch(appMaster)

  private val registerAppMaster = RegisterAppMaster(appId, appMaster, appContext.workerInfo)

  private val masterConnectionKeeper = context.actorOf(
    masterConnectionKeeperFactory(master, registerAppMaster, self))
  context.watch(masterConnectionKeeper)

  def receive: Receive = {
    case MasterConnected =>
      LOG.info(s"Master is connected, start AppMaster $appId...")
      appMaster ! StartAppMaster
    case MasterStopped =>
      LOG.error(s"Master is stopped, stop AppMaster $appId...")
      context.stop(self)
    case Terminated(actor) => actor match {
      case `appMaster` =>
        LOG.error(s"AppMaster $appId is stopped, shutdown myself")
        context.stop(self)
      case `masterConnectionKeeper` =>
        LOG.error(s"Master connection keeper is stopped, appId: $appId, shutdown myself")
        context.stop(self)
      case _ => // Skip
    }
  }
}

object AppMasterRuntimeEnvironment {

  def props(masters: Iterable[ActorPath], app: AppDescription, appContextInput: AppMasterContext
      ): Props = {

    val master = (appId: AppId, masterProxy: MasterActorRef) =>
      MasterWithExecutorSystemProvider.props(appId, masterProxy)

    val appMaster = (appContext: AppMasterContext, app: AppDescription) =>
      LazyStartAppMaster.props(appContext, app)

    val masterConnectionKeeper = (master: MasterActorRef, registerAppMaster:
      RegisterAppMaster, listener: ListenerActorRef) => Props(new MasterConnectionKeeper(
        registerAppMaster, master, masterStatusListener = listener))

    Props(new AppMasterRuntimeEnvironment(appContextInput, app, masters,
      master, appMaster, masterConnectionKeeper))
  }

  /**
   * This behavior like a AppMaster. Under the hood, It start start the real AppMaster in a lazy
   * way. When real AppMaster is not started yet, all messages are stashed. The stashed
   * messages are forwarded to real AppMaster when the real AppMaster is started.
   *
   * Please use LazyStartAppMaster.props to construct this actor
   *
   * @param appMasterProps  underlying AppMaster Props
   */
  private[appmaster]
  class LazyStartAppMaster(appId: Int, appMasterProps: Props) extends Actor with Stash {

    private val LOG = LogUtil.getLogger(getClass, app = appId)

    def receive: Receive = null

    context.become(startAppMaster)

    def startAppMaster: Receive = {
      case StartAppMaster =>
        val appMaster = context.actorOf(appMasterProps, "appmaster")
        context.watch(appMaster)
        context.become(terminationWatch(appMaster) orElse appMasterService(appMaster))
        unstashAll()
      case _ =>
        stash()
    }

    def terminationWatch(appMaster: ActorRef): Receive = {
      case Terminated(appMaster) =>
        LOG.error("appmaster is stopped")
        context.stop(self)
    }

    def appMasterService(appMaster: ActorRef): Receive = {
      case msg => appMaster forward msg
    }
  }

  private[appmaster]
  object LazyStartAppMaster {
    def props(appContext: AppMasterContext, app: AppDescription): Props = {
      val appMasterProps = Props(Class.forName(app.appMaster), appContext, app)
      Props(new LazyStartAppMaster(appContext.appId, appMasterProps))
    }
  }

  private[appmaster] case object StartAppMaster

  /**
   * This enhance Master by providing new service: StartExecutorSystems
   *
   * Please use MasterWithExecutorSystemProvider.props to construct this actor
   *
   */
  private[appmaster]
  class MasterWithExecutorSystemProvider(master: ActorRef, executorSystemProviderProps: Props)
    extends Actor {

    val executorSystemProvider: ActorRef = context.actorOf(executorSystemProviderProps)

    override def receive: Receive = {
      case request: StartExecutorSystems =>
        executorSystemProvider forward request
      case msg =>
        master forward msg
    }
  }

  private[appmaster]
  object MasterWithExecutorSystemProvider {
    def props(appId: Int, master: ActorRef): Props = {

      val executorSystemLauncher = (appId: Int, session: Session) =>
        Props(new ExecutorSystemLauncher(appId, session))

      val scheduler = Props(new ExecutorSystemScheduler(appId, master, executorSystemLauncher))

      Props(new MasterWithExecutorSystemProvider(master, scheduler))
    }
  }

  private[appmaster] type AppId = Int
  private[appmaster] type MasterActorRef = ActorRef
  private[appmaster] type ListenerActorRef = ActorRef
}