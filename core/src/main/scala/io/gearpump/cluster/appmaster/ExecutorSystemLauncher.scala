/*
 * Licensed under the Apache License, Version 2.0 (the
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

import akka.actor._
import io.gearpump.cluster.AppMasterToWorker.LaunchExecutor
import io.gearpump.cluster.ExecutorJVMConfig
import io.gearpump.cluster.WorkerToAppMaster._
import io.gearpump.cluster.appmaster.ExecutorSystemScheduler.{ExecutorSystemJvmConfig, Session}
import io.gearpump.cluster.scheduler.Resource
import io.gearpump.util.{ActorSystemBooter, ActorUtil, Constants, LogUtil}
import io.gearpump.util.ActorSystemBooter.{ActorSystemRegistered, RegisterActorSystem}
import org.slf4j.Logger
import scala.concurrent.duration._

/**
 * This launches single executor system on target worker.
 *
 * Please use ExecutorSystemLauncher.props() to construct this actor
 *
 * @param session The session that request to launch executor system
 */
private[appmaster]
class ExecutorSystemLauncher(appId: Int, session: Session) extends Actor {
  import ExecutorSystemLauncher._

  private val LOG: Logger = LogUtil.getLogger(getClass)

  val scheduler = context.system.scheduler
  implicit val executionContext = context.dispatcher

  private val systemConfig = context.system.settings.config
  val timeoutSetting = systemConfig.getInt(Constants.GEARPUMP_START_EXECUTOR_SYSTEM_TIMEOUT_MS)

  val timeout = scheduler.scheduleOnce(timeoutSetting.milliseconds,
    self, LaunchExecutorSystemTimeout(session))

  def receive: Receive = waitForLaunchCommand

  def waitForLaunchCommand: Receive = {
    case LaunchExecutorSystem(worker, executorSystemId, resource) =>
      val launcherPath = ActorUtil.getFullPath(context.system, self.path)
      val jvmConfig = Option(session.executorSystemJvmConfig)
        .map(getExecutorJvmConfig(_, s"app${appId}system${executorSystemId}", launcherPath)).orNull

      val launch = LaunchExecutor(appId, executorSystemId, resource, jvmConfig)
      LOG.info(s"Launching Executor ...appId: $appId, executorSystemId: $executorSystemId, " +
        s"slots: ${resource.slots} on worker $worker")

      worker.ref ! launch
      context.become(waitForActorSystemToStart(sender, launch, worker, executorSystemId))
  }

  def waitForActorSystemToStart(
      replyTo: ActorRef, launch: LaunchExecutor, worker: WorkerInfo, executorSystemId: Int)
    : Receive = {
    case RegisterActorSystem(systemPath) =>
      import launch._
      timeout.cancel()
      LOG.info(s"Received RegisterActorSystem $systemPath for session ${session.requestor}")
      sender ! ActorSystemRegistered(worker.ref)
      val system =
        ExecutorSystem(executorId, AddressFromURIString(systemPath), sender, resource, worker)
      replyTo ! LaunchExecutorSystemSuccess(system, session)
      context.stop(self)
    case ExecutorLaunchRejected(reason, ex) =>
      LOG.error(s"Executor Launch ${launch.resource} failed reason: $reason", ex)
      replyTo ! LaunchExecutorSystemRejected(launch.resource, reason, session)
      context.stop(self)
    case timeout: LaunchExecutorSystemTimeout =>
      LOG.error(s"The Executor ActorSystem $executorSystemId has not been started in time")
      replyTo ! timeout
      context.stop(self)
  }
}

private[appmaster]
object ExecutorSystemLauncher {

  case class LaunchExecutorSystem(worker: WorkerInfo, systemId: Int, resource: Resource)

  case class LaunchExecutorSystemSuccess(system: ExecutorSystem, session: Session)

  case class LaunchExecutorSystemRejected(resource: Resource, reason: Any, session: Session)

  case class LaunchExecutorSystemTimeout(session: Session)

  private def getExecutorJvmConfig(conf: ExecutorSystemJvmConfig, systemName: String,
      reportBack: String): ExecutorJVMConfig = {
    Option(conf).map { conf =>
      import conf._
      ExecutorJVMConfig(classPath, jvmArguments, classOf[ActorSystemBooter].getName,
        Array(systemName, reportBack), jar, username, executorAkkaConfig)
    }.orNull
  }
}
