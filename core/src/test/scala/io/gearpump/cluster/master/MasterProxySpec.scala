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

import org.apache.pekko.actor.{ActorIdentity, Cancellable, Props}
import org.apache.pekko.testkit.TestProbe
import com.typesafe.config.Config
import io.gearpump.cluster.{MasterHarness, TestUtil}
import io.gearpump.cluster.ClientToMaster.ShutdownApplication
import io.gearpump.security.ControlRequest
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration._

class MasterProxySpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with MasterHarness {
  override def config: Config = TestUtil.DEFAULT_CONFIG
  override def beforeAll(): Unit = startActorSystem()
  override def afterAll(): Unit = shutdownActorSystem()
  it should "keep privileged messages queued until a configured master is discovered" in {
    val master = TestProbe()(getActorSystem)
    val attacker = TestProbe()(getActorSystem)
    val client = TestProbe()(getActorSystem)
    val proxy = getActorSystem.actorOf(Props(new MasterProxy(Seq(master.ref.path), 10.seconds) {
      override def findMaster(): Cancellable = new Cancellable {
        def cancel(): Boolean = true
        def isCancelled: Boolean = false
      }
    }))
    attacker.send(proxy, ActorIdentity(None, Some(attacker.ref)))
    client.send(proxy, ShutdownApplication(1))
    attacker.expectNoMessage(200.millis)
    master.send(proxy, ActorIdentity(None, Some(master.ref)))
    master.expectMsgType[ControlRequest].message shouldBe ShutdownApplication(1)
  }
}
