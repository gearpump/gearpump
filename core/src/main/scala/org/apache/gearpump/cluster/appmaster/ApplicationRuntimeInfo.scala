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

import akka.actor.ActorRef
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.cluster.{ApplicationStatus, ApplicationTerminalStatus}

/** Run time info of Application */
case class ApplicationRuntimeInfo(
    appId: Int,
    // AppName is the unique Id for an application
    appName: String,
    appMaster: ActorRef = ActorRef.noSender,
    worker: ActorRef = ActorRef.noSender,
    user: String = "",
    submissionTime: TimeStamp = 0,
    startTime: TimeStamp = 0,
    finishTime: TimeStamp = 0,
    config: Config = ConfigFactory.empty(),
    status: ApplicationStatus = ApplicationStatus.NONEXIST) {

  def onAppMasterRegistered(appMaster: ActorRef, worker: ActorRef): ApplicationRuntimeInfo = {
    this.copy(appMaster = appMaster, worker = worker)
  }

  def onAppMasterActivated(timeStamp: TimeStamp): ApplicationRuntimeInfo = {
    this.copy(startTime = timeStamp, status = ApplicationStatus.ACTIVE)
  }

  def onFinalStatus(timeStamp: TimeStamp, finalStatus: ApplicationTerminalStatus):
    ApplicationRuntimeInfo = {
    this.copy(finishTime = timeStamp, status = finalStatus)
  }
}