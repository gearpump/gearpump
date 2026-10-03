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

import org.apache.pekko.actor.{Props, Status}
import org.apache.pekko.testkit.TestProbe
import com.typesafe.config.Config
import io.gearpump.cluster.{MasterHarness, TestUtil}
import io.gearpump.cluster.ClientToMaster._
import io.gearpump.security.{ControlCapability, ControlRequest, KvReply}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MasterControlSecuritySpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll
    with MasterHarness {
  override def config: Config = TestUtil.MASTER_CONFIG
  override def beforeAll(): Unit = startActorSystem()
  override def afterAll(): Unit = shutdownActorSystem()
  it should "deny unauthenticated submission and discovery without disrupting valid clients" in {
    val master = getActorSystem.actorOf(Props(new Master), "guarded-master")
    val client = TestProbe()(getActorSystem)
    client.send(master, GetJarStoreServer)
    client.expectMsgType[Status.Failure]
    client.send(master, SubmitApplication(TestUtil.dummyApp, None, "spoofed"))
    client.expectMsgType[Status.Failure]
    client.send(master, ControlRequest(ControlCapability.random(), None,
      SubmitApplication(TestUtil.dummyApp, None, "spoofed")))
    client.expectMsgType[Status.Failure]
    client.send(master, KvReply(ControlCapability.random(),
      InMemoryKVService.GetKVSuccess("next_worker_id", 999)))
    client.expectMsgType[Status.Failure]
    client.send(master, ControlCapability.wrap(config, GetJarStoreServer))
    assert(client.expectMsgType[JarStoreServerAddress].url.startsWith("https://"))
    getActorSystem.stop(master)
  }
}
