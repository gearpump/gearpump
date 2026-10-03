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

package io.gearpump.cluster.master

import org.apache.pekko.actor.Props
import org.apache.pekko.testkit.TestProbe
import com.typesafe.config.Config
import io.gearpump.cluster.{MasterHarness, TestUtil}
import io.gearpump.cluster.AppMasterToMaster.RequestResource
import io.gearpump.cluster.MasterToAppMaster.ResourceAllocated
import io.gearpump.cluster.scheduler.{Resource, ResourceAllocation, ResourceRequest}
import io.gearpump.cluster.worker.WorkerId
import io.gearpump.security._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration._

class AllocationBrokerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll
    with MasterHarness {
  override def config: Config = TestUtil.DEFAULT_CONFIG
  override def beforeAll(): Unit = startActorSystem()
  override def afterAll(): Unit = shutdownActorSystem()
  it should "install grants for allocations returned by the real priority scheduler" in {
    val scheduler = getActorSystem.actorOf(Props(new io.gearpump.cluster.scheduler.PriorityScheduler))
    val worker = TestProbe()(getActorSystem)
    val launcher = TestProbe()(getActorSystem)
    val workerId = WorkerId(1, 0)
    worker.send(scheduler, io.gearpump.cluster.MasterToWorker.WorkerRegistered(workerId,
      Master.MasterInfo(launcher.ref)))
    worker.send(scheduler, io.gearpump.cluster.WorkerToMaster.ResourceUpdate(worker.ref, workerId,
      Resource(10)))
    worker.expectMsgType[ControlRequest]
    val app = ControlCapability.random()
    val request = RequestResource(1, ResourceRequest(Resource(2), WorkerId.unspecified))
    val broker = getActorSystem.actorOf(Props(new AllocationBroker(
      AllocateResource(request, app), scheduler, launcher.ref)))
    val install = worker.expectMsgType[InstallLaunchGrant](3.seconds)
    worker.reply(LaunchGrantInstalled(install.grant))
    launcher.expectMsgType[ResourceAllocated](3.seconds).allocations.head.allocationCapability shouldBe install.grant
    getActorSystem.stop(broker)
    getActorSystem.stop(scheduler)
  }

  it should "install worker authority before returning an allocation to its launcher" in {
    val scheduler = TestProbe()(getActorSystem)
    val worker = TestProbe()(getActorSystem)
    val launcher = TestProbe()(getActorSystem)
    val app = ControlCapability.random()
    val request = RequestResource(1, ResourceRequest(Resource(2), WorkerId.unspecified))
    val broker = getActorSystem.actorOf(Props(new AllocationBroker(
      AllocateResource(request, app), scheduler.ref, launcher.ref)))
    launcher.watch(broker)
    scheduler.expectMsg(request)
    scheduler.reply(ResourceAllocated(Array(ResourceAllocation(Resource(2), worker.ref, WorkerId(1, 0)))))
    val install = worker.expectMsgType[InstallLaunchGrant]
    assert(install.appId == 1 && install.slots == 2 && install.applicationCapability == app)
    assert(ControlCapability.matches(ControlCapability.token(config, ControlCapability.AdminKey),
      install.capability))
    launcher.expectNoMessage(100.millis)
    worker.reply(LaunchGrantInstalled(install.grant))
    assert(launcher.expectMsgType[ResourceAllocated].allocations.head.allocationCapability == install.grant)
    launcher.expectTerminated(broker)
  }
}
