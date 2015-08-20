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

package io.gearpump.cluster.client

import akka.actor.ActorRef
import akka.pattern.ask
import io.gearpump.cluster.ClientToMaster._
import io.gearpump.cluster.MasterToAppMaster.{AppMastersData, AppMastersDataRequest, ReplayFromTimestampWindowTrailingEdge}
import io.gearpump.cluster.MasterToClient.{ReplayApplicationResult, ResolveAppIdResult, ShutdownApplicationResult, SubmitApplicationResult}
import io.gearpump.cluster.{AppJar, AppDescription}
import io.gearpump.util.{ActorUtil, Constants}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.util.{Failure, Success}

/**
 * Stateless, thread safe
 */
class MasterClient(master : ActorRef) {
  implicit val timeout = Constants.FUTURE_TIMEOUT

  def submitApplication(app : AppDescription, appJar: Option[AppJar]) : Int = {
    val result = Await.result( (master ? SubmitApplication(app, appJar)).asInstanceOf[Future[SubmitApplicationResult]], Duration.Inf)
    val appId = result.appId match {
      case Success(appId) =>
        Console.println(s"Submit application succeed. The application id is $appId")
        appId
      case Failure(ex) => throw ex
    }
    appId
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

  def listApplications: AppMastersData = {
    val result = Await.result((master ? AppMastersDataRequest).asInstanceOf[Future[AppMastersData]], Duration.Inf)
    result
  }

  /**
   * Internal API, will be removed in future
   */
  def replayFromTimestampWindowTrailingEdge(appId : Int): ReplayApplicationResult = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val result = Await.result(ActorUtil.askAppMaster[ReplayApplicationResult](master, appId,ReplayFromTimestampWindowTrailingEdge(appId)), Duration.Inf)
    result
  }
}