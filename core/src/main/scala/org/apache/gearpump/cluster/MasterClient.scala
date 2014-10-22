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

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import org.apache.gearpump.cluster.ClientToMaster._
import org.apache.gearpump.cluster.MasterToAppMaster.{ReplayAppFromLatestTimestamp, AppMastersData, AppMastersDataRequest}
import org.apache.gearpump.cluster.MasterToClient.{ReplayApplicationResult, ShutdownApplicationResult, SubmitApplicationResult}
import org.apache.gearpump.util.Configs

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

class MasterClient(master : ActorRef) {
  private implicit val timeout = Timeout(5, TimeUnit.SECONDS)

  def submitApplication(appMaster : Class[_ <: Actor], config : Configs, app : Application) : Int = {
    val result = Await.result( (master ? SubmitApplication(appMaster, config, app)).asInstanceOf[Future[SubmitApplicationResult]], Duration.Inf)
    result.appId match {
      case Success(appId) => appId
      case Failure(ex) => throw(ex)
    }
  }

  def shutdownApplication(appId : Int) : Unit = {
    val result = Await.result((master ? ShutdownApplication(appId)).asInstanceOf[Future[ShutdownApplicationResult]], Duration.Inf)
    result.appId match {
      case Success(_) => Unit
      case Failure(ex) => throw(ex)
    }
  }

  def listApplications = {
    val result = Await.result((master ? AppMastersDataRequest).asInstanceOf[Future[AppMastersData]], Duration.Inf)
    result
  }

  def replayAppFromLatestTimestamp(appId : Int) = {
    val result = Await.result((master ? ReplayAppFromLatestTimestamp(appId)).asInstanceOf[Future[ReplayApplicationResult]], Duration.Inf)
    result
  }
}