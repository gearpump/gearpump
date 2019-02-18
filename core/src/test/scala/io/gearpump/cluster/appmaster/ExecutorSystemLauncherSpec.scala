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

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestProbe
import com.typesafe.config.ConfigValueFactory
import io.gearpump.cluster.AppMasterToWorker.LaunchExecutor
import io.gearpump.cluster.TestUtil
import io.gearpump.cluster.WorkerToAppMaster.ExecutorLaunchRejected
import io.gearpump.cluster.appmaster.ExecutorSystemLauncher._
import io.gearpump.cluster.appmaster.ExecutorSystemScheduler.Session
import io.gearpump.cluster.scheduler.Resource
import io.gearpump.cluster.worker.WorkerId
import io.gearpump.util.ActorSystemBooter.{ActorSystemRegistered, RegisterActorSystem}
import io.gearpump.util.Constants
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import scala.concurrent.Await
import scala.concurrent.duration._

class ExecutorSystemLauncherSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit var system: ActorSystem = null
  val workerId: WorkerId = WorkerId(0, 0L)
  val appId = 0
  val executorId = 0
  val url = "akka.tcp://worker@127.0.0.1:3000"
  val session = Session(null, null)
  val launchExecutorSystemTimeout = 3000
  val activeConfig = TestUtil.DEFAULT_CONFIG.
    withValue(Constants.GEARPUMP_START_EXECUTOR_SYSTEM_TIMEOUT_MS,
      ConfigValueFactory.fromAnyRef(launchExecutorSystemTimeout))

  override def beforeAll(): Unit = {
    system = ActorSystem("test", activeConfig)
  }

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }

  it should "report success when worker launch the system successfully" in {
    val worker = TestProbe()
    val client = TestProbe()

    val launcher = system.actorOf(Props(new ExecutorSystemLauncher(appId, session)))
    client.watch(launcher)
    client.send(launcher, LaunchExecutorSystem(WorkerInfo(workerId, worker.ref), 0, Resource(1)))

    worker.expectMsgType[LaunchExecutor]
    worker.reply(RegisterActorSystem(url))

    worker.expectMsgType[ActorSystemRegistered]

    client.expectMsgType[LaunchExecutorSystemSuccess]
    client.expectTerminated(launcher)
  }

  it should "report failure when worker refuse to launch the system explicitly" in {
    val worker = TestProbe()
    val client = TestProbe()

    val resource = Resource(4)

    val launcher = system.actorOf(Props(new ExecutorSystemLauncher(appId, session)))
    client.watch(launcher)
    client.send(launcher, LaunchExecutorSystem(WorkerInfo(workerId, worker.ref), 0, resource))

    worker.expectMsgType[LaunchExecutor]
    worker.reply(ExecutorLaunchRejected())

    client.expectMsg(LaunchExecutorSystemRejected(resource, null, session))
    client.expectTerminated(launcher)
  }

  it should "report timeout when trying to start a executor system on worker, " +
    "and worker doesn't response" in {
    val client = TestProbe()
    val worker = TestProbe()
    val launcher = system.actorOf(Props(new ExecutorSystemLauncher(appId, session)))
    client.send(launcher, LaunchExecutorSystem(WorkerInfo(workerId, worker.ref), 0, Resource(1)))
    client.watch(launcher)
    val waitFor = launchExecutorSystemTimeout + 10000
    client.expectMsgType[LaunchExecutorSystemTimeout](waitFor.milliseconds)
    client.expectTerminated(launcher, waitFor.milliseconds)
  }
}
