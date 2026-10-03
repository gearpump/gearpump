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

package io.gearpump.cluster.worker

import org.apache.pekko.actor.{Props, Status}
import org.apache.pekko.testkit.TestProbe
import com.typesafe.config.Config
import io.gearpump.cluster.{ExecutorJVMConfig, MasterHarness, TestUtil}
import io.gearpump.cluster.AppMasterToWorker.LaunchExecutor
import io.gearpump.cluster.MasterToWorker.WorkerRegistered
import io.gearpump.cluster.WorkerToMaster.{RegisterNewWorker, ResourceUpdate}
import io.gearpump.cluster.WorkerToAppMaster.ExecutorLaunchRejected
import io.gearpump.cluster.master.Master.MasterInfo
import io.gearpump.cluster.scheduler.Resource
import io.gearpump.security._
import io.gearpump.util.ActorSystemBooter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WorkerLaunchSecuritySpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll
    with MasterHarness {
  override def config: Config = TestUtil.DEFAULT_CONFIG
  override def beforeAll(): Unit = startActorSystem()
  override def afterAll(): Unit = shutdownActorSystem()
  it should "reject absent grants, invalid resources and malformed payloads before starting executors" in {
    val master = TestProbe()(getActorSystem)
    val attacker = TestProbe()(getActorSystem)
    val worker = getActorSystem.actorOf(Props(new Worker(master.ref)))
    master.expectMsg(RegisterNewWorker)
    attacker.send(worker, WorkerRegistered(WorkerId(1, 0), MasterInfo(attacker.ref)))
    attacker.expectMsgType[Status.Failure]
    master.send(worker, ControlRequest(ControlCapability.token(config, ControlCapability.AdminKey), None,
      WorkerRegistered(WorkerId(1, 0), MasterInfo(master.ref))))
    master.expectMsgType[ControlRequest]
    val capability = ControlCapability.random()
    val runtime = ControlCapability.runtimeConfig(config, 1, capability)
    val jvm = ExecutorJVMConfig(Array.empty[String], Array.empty[String],
      classOf[ActorSystemBooter].getName, Array("name", "reportBack"), None, "owner", runtime)
    Seq(LaunchExecutor(1, 1, Resource(0), jvm), LaunchExecutor(1, 2, Resource(-1), jvm),
      LaunchExecutor(1, 3, Resource(1), jvm), LaunchExecutor(1, 4, Resource(1), null),
      LaunchExecutor(1, 5, Resource(1), jvm.copy(arguments = null))).foreach { message =>
      attacker.send(worker, message)
      attacker.expectMsgType[ExecutorLaunchRejected]
    }
    attacker.send(worker, InstallLaunchGrant(ControlCapability.random(), ControlCapability.random(),
      1, 1, capability))
    attacker.expectMsgType[Status.Failure]
    // A valid grant still cannot authorize a different app or a zero-slot launch.
    val grant = ControlCapability.random()
    master.send(worker, InstallLaunchGrant(ControlCapability.token(config, ControlCapability.AdminKey),
      grant, 1, 1, capability))
    master.expectMsg(LaunchGrantInstalled(grant))
    attacker.send(worker, LaunchExecutor(2, 6, Resource(1), jvm, grant))
    attacker.expectMsgType[ExecutorLaunchRejected]
    attacker.send(worker, LaunchExecutor(1, 7, Resource(0), jvm, grant))
    attacker.expectMsgType[ExecutorLaunchRejected]
    master.expectNoMessage(scala.concurrent.duration.Duration(200, "millis"))
  }
}
