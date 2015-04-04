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
package org.apache.gearpump.examples.distributedshell

import akka.actor.{Deploy, Props}
import akka.remote.RemoteScope
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.gearpump.cluster.ClientToMaster.ShutdownApplication
import org.apache.gearpump.cluster.appmaster.ExecutorSystemScheduler.{ExecutorSystemJvmConfig, StartExecutorSystemTimeout, ExecutorSystemStarted}
import org.apache.gearpump.cluster.{ApplicationMaster, ExecutorContext, AppMasterContext, AppDescription}
import org.apache.gearpump.examples.distributedshell.DistShellAppMaster._
import org.apache.gearpump.util.{LogUtil, Constants, ActorUtil, Util}
import org.slf4j.Logger

import scala.concurrent.Future
import akka.pattern.{ask, pipe}

class DistShellAppMaster(appContext : AppMasterContext, app : AppDescription) extends ApplicationMaster {
  import appContext._
  import context.dispatcher
  implicit val timeout = Constants.FUTURE_TIMEOUT
  private val LOG: Logger = LogUtil.getLogger(getClass, app = appId)
  protected var currentExecutorId = 0

  override def preStart(): Unit = {
    LOG.info(s"Distributed Shell AppMaster started")
    ActorUtil.launchExecutorOnEachWorker(masterProxy, getExecutorJvmConfig, self)
  }

  override def receive: Receive = {
    case ExecutorSystemStarted(executorSystem) =>
      import executorSystem.{address, worker, resource => executorResource}
      val executorContext = ExecutorContext(currentExecutorId, worker.workerId, appId, self, executorResource)
      //start executor
      val executor = context.actorOf(Props(classOf[ShellExecutor], executorContext, app.userConfig)
          .withDeploy(Deploy(scope = RemoteScope(address))), currentExecutorId.toString)
      executorSystem.bindLifeCycleWith(executor)
      currentExecutorId += 1
    case StartExecutorSystemTimeout =>
      LOG.error(s"Failed to allocate resource in time")
      masterProxy ! ShutdownApplication(appId)
      context.stop(self)
    case msg: ShellCommand =>
      Future.fold(context.children.map(_ ? msg))(new ShellCommandResultAggregator) { (aggregator, response) =>
        aggregator.aggregate(response.asInstanceOf[ShellCommandResult])
      }.map(_.toString()) pipeTo sender
  }

  private def getExecutorJvmConfig: ExecutorSystemJvmConfig = {
    val config: Config = Option(app.clusterConfig).map(_.getConfig).getOrElse(ConfigFactory.empty())
    val jvmSetting = Util.resolveJvmSetting(config.withFallback(context.system.settings.config)).executor
    ExecutorSystemJvmConfig(jvmSetting.classPath, jvmSetting.vmargs,
      appJar, username, config)
  }
}

object DistShellAppMaster {
  case class ShellCommand(command: String)

  case class ShellCommandResult(executorId: Int, msg: Any)

  class ShellCommandResultAggregator {
    val result: StringBuilder = new StringBuilder

    def aggregate(response: ShellCommandResult): ShellCommandResultAggregator = {
      result.append(s"Execute results from executor ${response.executorId} : \n")
      result.append(response.msg + "\n")
      this
    }

    override def toString() = result.toString()
  }
}