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
package org.apache.gearpump.experiments.cluster.appmaster

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.remote.RemoteScope
import org.apache.gearpump.cluster.AppMasterToWorker.LaunchExecutor
import org.apache.gearpump.util.ActorSystemBooter.{ActorSystemRegistered, BindLifeCycle, RegisterActorSystem}
import org.apache.gearpump.util._
import org.slf4j.Logger

import scala.concurrent.duration.Duration
import org.apache.gearpump.cluster.{UserConfig, ExecutorContext}

object LaunchActorSystemTimeOut
case object AllocateResourceTimeOut
case class LaunchExecutorActor(executorConfig : Props, executorId : Int, daemon: ActorRef)

class ExecutorLauncher (executorClass: Class[_ <: Actor], worker : ActorRef, launch : LaunchExecutor, executorConfig : ExecutorContext, userConf : UserConfig) extends Actor {

  import executorConfig._

  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId, executor = executorId)

  worker ! launch

  implicit val executionContext = context.dispatcher
  val timeout = context.system.scheduler.scheduleOnce(Duration(15, TimeUnit.SECONDS), self, LaunchActorSystemTimeOut)

  def receive : Receive = waitForActorSystemToStart

  def waitForActorSystemToStart : Receive = {
    case RegisterActorSystem(systemPath) =>
      timeout.cancel()
      sender ! ActorSystemRegistered(worker)
      LOG.info(s"Received RegisterActorSystem $systemPath for app master")
      val executorProps = Props(executorClass, executorConfig, userConf).withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(systemPath))))

      context.parent ! LaunchExecutorActor(executorProps, executorConfig.executorId, sender())
      context.stop(self)
    case LaunchActorSystemTimeOut =>
      LOG.error("The Executor ActorSystem has not been started in time, cannot start Executor" +
        "in it...")
      context.stop(self)
  }
}
