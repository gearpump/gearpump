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

package io.gearpump.cluster.appmaster

import java.util.concurrent.TimeUnit

import akka.actor._
import io.gearpump.cluster.master.MasterProxy.MasterRestarted
import io.gearpump.cluster.AppMasterToMaster.RegisterAppMaster
import io.gearpump.cluster.MasterToAppMaster.AppMasterRegistered
import io.gearpump.cluster.appmaster.MasterConnectionKeeper.AppMasterRegisterTimeout
import io.gearpump.cluster.appmaster.MasterConnectionKeeper.MasterConnectionStatus.{MasterConnected, MasterStopped}
import io.gearpump.cluster.master.MasterProxy.WatchMaster
import io.gearpump.util.LogUtil

import scala.concurrent.duration.FiniteDuration

/**
 * This will watch the liveness of Master.
 * When Master is restarted, it will send RegisterAppMaster to the new Master instance.
 * If Master is stopped, it will send the MasterConnectionStatus to listener
 *
 * please use MasterConnectionKeeper.props() to construct this actor
 *
 * @param register
 * @param masterProxy
 * @param masterStatusListener
 */
private[appmaster]
class MasterConnectionKeeper (
    register: RegisterAppMaster, masterProxy: ActorRef, masterStatusListener: ActorRef)
  extends Actor {

  import context.dispatcher

  private val LOG = LogUtil.getLogger(getClass)
  private var master: ActorRef = null

  //Subscribe self to masterProxy,
  masterProxy ! WatchMaster(self)

  def registerAppMaster: Cancellable = {
    masterProxy ! register
    context.system.scheduler.scheduleOnce(FiniteDuration(30, TimeUnit.SECONDS), self, AppMasterRegisterTimeout)
  }

  context.become(waitMasterToConfirm(registerAppMaster))

  def waitMasterToConfirm(cancelRegister: Cancellable): Receive = {
    case AppMasterRegistered(appId) =>
      cancelRegister.cancel()
      masterStatusListener ! MasterConnected
      context.become(masterLivenessListener)
    case AppMasterRegisterTimeout =>
      cancelRegister.cancel()
      masterStatusListener ! MasterStopped
      context.stop(self)
  }

  def masterLivenessListener: Receive = {
    case MasterRestarted =>
      LOG.info("Master restarted, re-registering appmaster....")
      context.become(waitMasterToConfirm(registerAppMaster))
    case MasterStopped =>
      LOG.info("Master is dead, killing this AppMaster....")
      masterStatusListener ! MasterStopped
      context.stop(self)
  }

  def receive: Receive = null
}

private[appmaster] object MasterConnectionKeeper {
  case object AppMasterRegisterTimeout

  object MasterConnectionStatus {
    case object MasterConnected
    case object MasterStopped
  }
}