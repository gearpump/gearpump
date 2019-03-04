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

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.TestProbe
import io.gearpump.cluster.{AppJar, TestUtil}
import io.gearpump.cluster.AppMasterToMaster.RequestResource
import io.gearpump.cluster.MasterToAppMaster.ResourceAllocated
import io.gearpump.cluster.appmaster.ExecutorSystemLauncher._
import io.gearpump.cluster.appmaster.ExecutorSystemScheduler._
import io.gearpump.cluster.appmaster.ExecutorSystemSchedulerSpec.{ExecutorSystemLauncherStarted, MockExecutorSystemLauncher}
import io.gearpump.cluster.scheduler.{Resource, ResourceAllocation, ResourceRequest}
import io.gearpump.cluster.worker.WorkerId
import io.gearpump.jarstore.FilePath
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import scala.concurrent.Await
import scala.concurrent.duration._

class ExecutorSystemSchedulerSpec extends FlatSpec with Matchers with BeforeAndAfterEach {
  val appId = 0
  val workerId = WorkerId(0, 0L)
  val resource = Resource(1)
  val resourceRequest = ResourceRequest(resource, WorkerId.unspecified)
  val mockJar = AppJar("for_test", FilePath("PATH"))
  val emptyJvmConfig = ExecutorSystemJvmConfig(Array.empty, Array.empty, Some(mockJar), "")
  val start = StartExecutorSystems(Array(resourceRequest), emptyJvmConfig)

  implicit var system: ActorSystem = null
  var worker: TestProbe = null
  var workerInfo: WorkerInfo = null
  var masterProxy: TestProbe = null
  var launcher: TestProbe = null
  var client: TestProbe = null

  override def beforeEach(): Unit = {
    system = ActorSystem("test", TestUtil.DEFAULT_CONFIG)
    worker = TestProbe()
    workerInfo = WorkerInfo(workerId, worker.ref)
    masterProxy = TestProbe()
    launcher = TestProbe()
    client = TestProbe()

    val scheduler = system.actorOf(
      Props(new ExecutorSystemScheduler(appId, masterProxy.ref, (_: Int, session: Session) => {
        Props(new MockExecutorSystemLauncher(launcher, session))
      })))

    client.send(scheduler, start)
    masterProxy.expectMsg(RequestResource(appId, resourceRequest))
  }

  override def afterEach(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }

  private def launcherStarted(launcher: TestProbe): Option[ExecutorSystemLauncherStarted] = {
    val launcherStarted = launcher.receiveOne(15.seconds)

    launcherStarted match {
      case start: ExecutorSystemLauncherStarted => Some(start)
      case _ =>
        assert(false, "ExecutorSystemLauncherStarted == false")
        None
    }
  }

  it should "schedule and launch an executor system on target worker successfully" in {

    masterProxy.reply(ResourceAllocated(Array(ResourceAllocation(resource, worker.ref, workerId))))

    val ExecutorSystemLauncherStarted(session) = launcherStarted(launcher).get

    val systemId = 0
    launcher.expectMsg(LaunchExecutorSystem(workerInfo, systemId, resource))

    val executorSystemProbe = TestProbe()
    val executorSystem =
      ExecutorSystem(systemId, null, executorSystemProbe.ref, resource, workerInfo)
    launcher.reply(LaunchExecutorSystemSuccess(executorSystem, session))
    client.expectMsg(ExecutorSystemStarted(executorSystem, Some(mockJar)))
  }

  it should "report failure when resource cannot be allocated" in {
    client.expectMsg(30.seconds, StartExecutorSystemTimeout)
  }

  it should "schedule new resouce on new worker " +
    "when target worker reject creating executor system" in {
    masterProxy.reply(ResourceAllocated(Array(ResourceAllocation(resource, worker.ref, workerId))))
    val ExecutorSystemLauncherStarted(session) = launcherStarted(launcher).get

    val systemId = 0
    launcher.expectMsg(LaunchExecutorSystem(workerInfo, systemId, resource))
    launcher.reply(LaunchExecutorSystemRejected(resource, "", session))
    masterProxy.expectMsg(RequestResource(appId, resourceRequest))
  }

  it should "report failure when resource is allocated, but timeout " +
    "when starting the executor system" in {
    masterProxy.reply(ResourceAllocated(Array(ResourceAllocation(resource, worker.ref, workerId))))
    val ExecutorSystemLauncherStarted(session) = launcherStarted(launcher).get

    val systemId = 0
    launcher.expectMsg(LaunchExecutorSystem(workerInfo, systemId, resource))
    launcher.reply(LaunchExecutorSystemTimeout(session))
    client.expectMsg(StartExecutorSystemTimeout)
  }
}

object ExecutorSystemSchedulerSpec {
  class MockExecutorSystemLauncher(forwardTo: TestProbe, session: Session) extends Actor {
    forwardTo.ref ! ExecutorSystemLauncherStarted(session)

    def receive: Receive = {
      case msg => forwardTo.ref forward msg
    }
  }

  case class ExecutorSystemLauncherStarted(session: Session)
}