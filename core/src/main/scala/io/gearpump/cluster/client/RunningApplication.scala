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
package io.gearpump.cluster.client

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import io.gearpump.cluster.ClientToMaster.{RegisterAppResultListener, ResolveAppId, ShutdownApplication}
import io.gearpump.cluster.MasterToClient._
import io.gearpump.cluster.client.RunningApplication._
import io.gearpump.util.{ActorUtil, LogUtil}
import java.time.Duration
import java.util.concurrent.TimeUnit
import org.slf4j.Logger
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
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
   * This function will block until the application finished or failed.
   * If failed, an exception will be thrown out
   */
  def waitUntilFinish(): Unit = {
    this.waitUntilFinish(INF_DURATION)
  }

  def waitUntilFinish(duration: Duration): Unit = {
    val result = ActorUtil.askActor[ApplicationResult](master,
      RegisterAppResultListener(appId), new Timeout(duration.getSeconds, TimeUnit.SECONDS))
    if (result.appId == appId) {
      result match {
        case failed: ApplicationFailed =>
          throw failed.error
        case _: ApplicationSucceeded =>
          LOG.info(s"Application $appId succeeded")
        case _: ApplicationTerminated =>
          LOG.info(s"Application $appId terminated")
      }
    } else {
      LOG.warn(s"Received unexpected result $result for application $appId")
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
  private val INF_DURATION = Duration.ofSeconds(2147482)
}

