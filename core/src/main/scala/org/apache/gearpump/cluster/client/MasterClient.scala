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

package org.apache.gearpump.cluster.client

import akka.actor.ActorRef
import akka.pattern.ask
import org.apache.gearpump.cluster.ClientToMaster._
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMastersData, AppMastersDataRequest, ReplayFromTimestampWindowTrailingEdge}
import org.apache.gearpump.cluster.MasterToClient.{ReplayApplicationResult, ResolveAppIdResult, ShutdownApplicationResult, SubmitApplicationResult}
import org.apache.gearpump.cluster.{AppJar, AppDescription}
import org.apache.gearpump.util.Constants

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

/**
 * Stateless, thread safe
 */
class MasterClient(master : ActorRef) {
  implicit val timeout = Constants.FUTURE_TIMEOUT

  def submitApplication(app : AppDescription, appJar: Option[AppJar]) : Int = {
    val result = Await.result( (master ? SubmitApplication(app, appJar)).asInstanceOf[Future[SubmitApplicationResult]], Duration.Inf)
    result.appId match {
      case Success(appId) => appId
      case Failure(ex) => throw ex
    }
  }

  def resolveAppId(appId: Int): ActorRef = {
    val result = Await.result((master ? ResolveAppId(appId)).asInstanceOf[Future[ResolveAppIdResult]], Duration.Inf)
    result.appMaster match {
      case Success(appMaster) => appMaster
      case Failure(ex) => throw ex
    }
  }

  def shutdownApplication(appId : Int) : Unit = {
    val result = Await.result((master ? ShutdownApplication(appId)).asInstanceOf[Future[ShutdownApplicationResult]], Duration.Inf)
    result.appId match {
      case Success(_) =>
      case Failure(ex) => throw ex
    }
  }

  def listApplications = {
    val result = Await.result((master ? AppMastersDataRequest).asInstanceOf[Future[AppMastersData]], Duration.Inf)
    result
  }

  /**
   * Internal API, will be removed in future
   */
  def replayFromTimestampWindowTrailingEdge(appId : Int) = {
    val result = Await.result((master ? ReplayFromTimestampWindowTrailingEdge(appId)).asInstanceOf[Future[ReplayApplicationResult]], Duration.Inf)
    result
  }
}