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

package io.gearpump.streaming.appmaster

import akka.actor._
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import io.gearpump.TestProbeUtil
import io.gearpump.cluster.{TestUtil, UserConfig, _}
import io.gearpump.cluster.AppMasterToWorker.ChangeExecutorResource
import io.gearpump.cluster.appmaster.{ExecutorSystem, WorkerInfo}
import io.gearpump.cluster.appmaster.ExecutorSystemScheduler.{ExecutorSystemStarted, StartExecutorSystems, StartExecutorSystemTimeout}
import io.gearpump.cluster.scheduler.{Resource, ResourceRequest}
import io.gearpump.cluster.worker.WorkerId
import io.gearpump.jarstore.FilePath
import io.gearpump.streaming.ExecutorId
import io.gearpump.streaming.ExecutorToAppMaster.RegisterExecutor
import io.gearpump.streaming.appmaster.ExecutorManager.{ExecutorStarted, _}
import io.gearpump.streaming.appmaster.ExecutorManagerSpec.StartExecutorActorPlease
import io.gearpump.util.ActorSystemBooter.BindLifeCycle
import io.gearpump.util.LogUtil
import org.scalatest._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ExecutorManagerSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit var system: ActorSystem = null

  private val LOG = LogUtil.getLogger(getClass)
  private val appId = 0
  private val resource = Resource(10)

  override def beforeAll(): Unit = {
    system = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
  }

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }

  private def startExecutorSystems = {
    val master = TestProbe()
    val taskManager = TestProbe()
    val executor = TestProbe()
    val userConfig = UserConfig.empty

    val username = "user"
    val appName = "app"
    val appJar = Some(AppJar("for_test", FilePath("path")))

    val appMasterContext = AppMasterContext(appId, username, null, null, appJar, master.ref)

    val executorFactory = (_: ExecutorContext, _: UserConfig, _: Address, _: ExecutorId) => {
      executor.ref ! StartExecutorActorPlease
      TestProbeUtil.toProps(executor)
    }
    val executorManager = system.actorOf(Props(new ExecutorManager(userConfig, appMasterContext,
      executorFactory, ConfigFactory.empty, appName)))

    taskManager.send(executorManager, SetTaskManager(taskManager.ref))
    val resourceRequest = Array(ResourceRequest(resource, WorkerId.unspecified))

    // Starts executors
    taskManager.send(executorManager, StartExecutors(resourceRequest, appJar.get))

    // Asks master to start executor systems
    import scala.concurrent.duration._
    val startExecutorSystem = master.receiveOne(5.seconds).asInstanceOf[StartExecutorSystems]
    assert(startExecutorSystem.resources == resourceRequest)
    import startExecutorSystem.executorSystemConfig.{classPath, executorAkkaConfig, jar, jvmArguments, username => returnedUserName}
    assert(startExecutorSystem.resources == resourceRequest)

    assert(classPath.length == 0)
    assert(jvmArguments.length == 0)
    assert(jar == appJar)
    assert(returnedUserName == username)
    assert(executorAkkaConfig.isEmpty)

    (master, executor, taskManager, executorManager)
  }

  it should "report timeout to taskManager" in {
    import io.gearpump.streaming.appmaster.ExecutorManager._
    val (master, _, taskManager, _) = startExecutorSystems
    master.reply(StartExecutorSystemTimeout)
    taskManager.expectMsg(StartExecutorsTimeOut)
  }

  it should "start executor actor correctly" in {
    val (master, executor, taskManager, executorManager) = startExecutorSystems
    val executorSystemDaemon = TestProbe()
    val worker = TestProbe()
    val workerId = WorkerId(0, 0L)
    val workerInfo = WorkerInfo(workerId, worker.ref)
    val executorSystem = ExecutorSystem(0, null, executorSystemDaemon.ref,
      resource, workerInfo)
    master.reply(ExecutorSystemStarted(executorSystem, None))
    import scala.concurrent.duration._
    val bindLifeWith = executorSystemDaemon.receiveOne(3.seconds).asInstanceOf[BindLifeCycle]
    val proxyExecutor = bindLifeWith.actor
    executor.expectMsg(StartExecutorActorPlease)

    val executorId = 0

    // Registers executor
    executor.send(executorManager, RegisterExecutor(proxyExecutor, executorId,
      resource, workerInfo))
    taskManager.expectMsgType[ExecutorStarted]

    // Broadcasts message to childs
    taskManager.send(executorManager, BroadCast("broadcast"))
    executor.expectMsg("broadcast")

    // Unicast
    taskManager.send(executorManager, UniCast(executorId, "unicast"))
    executor.expectMsg("unicast")

    // Updates executor resource status
    val usedResource = Resource(5)
    executorManager ! ExecutorResourceUsageSummary(Map(executorId -> usedResource))
    worker.expectMsg(ChangeExecutorResource(appId, executorId, resource - usedResource))

    // Watches for executor termination
    system.stop(executor.ref)
    LOG.info("Shutting down executor, and wait taskManager to get notified")
    taskManager.expectMsg(ExecutorStopped(executorId))
  }
}

object ExecutorManagerSpec {
  case object StartExecutorActorPlease
}
