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
package org.apache.gearpump.cluster.client

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import org.apache.gearpump.cluster.ClientToMaster.{RegisterAppResultListener, ResolveAppId, ShutdownApplication}
import org.apache.gearpump.cluster.MasterToClient._
import org.apache.gearpump.cluster.client.RunningApplication._
import org.apache.gearpump.util.{ActorUtil, LogUtil}
import org.slf4j.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class RunningApplication(val appId: Int, master: ActorRef, timeout: Timeout) {
  lazy val appMaster: Future[ActorRef] = resolveAppMaster(appId)

  def shutDown(): Unit = {
    val result = ActorUtil.askActor[ShutdownApplicationResult](master,
      ShutdownApplication(appId), timeout)
    result.appId match {
      case Success(_) =>
      case Failure(ex) => throw ex
    }
  }

  /**
   * This funtion will block until the application finished or failed.
   * If failed, an exception will be thrown out
   */
  def waitUntilFinish(): Unit = {
    val result = ActorUtil.askActor[ApplicationResult](master,
      RegisterAppResultListener(appId), INF_TIMEOUT)
    result match {
      case failed: ApplicationFailed =>
        throw failed.error
      case _ =>
        LOG.info(s"Application $appId succeeded")
    }
  }

  def askAppMaster[T](msg: Any): Future[T] = {
    appMaster.flatMap(_.ask(msg)(timeout).asInstanceOf[Future[T]])
  }

  private def resolveAppMaster(appId: Int): Future[ActorRef] = {
    master.ask(ResolveAppId(appId))(timeout).
      asInstanceOf[Future[ResolveAppIdResult]].map(_.appMaster.get)
  }
}

object RunningApplication {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  // This magic number is derived from Akka's configuration, which is the maximum delay
  private val INF_TIMEOUT = new Timeout(2147482 seconds)
}

