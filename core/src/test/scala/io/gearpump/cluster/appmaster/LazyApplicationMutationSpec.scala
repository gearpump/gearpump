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

import org.apache.pekko.actor.{ActorSystem, Props, Status}
import org.apache.pekko.testkit.TestProbe
import io.gearpump.TestProbeUtil._
import io.gearpump.cluster.TestUtil
import io.gearpump.cluster.appmaster.AppMasterRuntimeEnvironment._
import io.gearpump.security._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.Await
import scala.concurrent.duration._

case object TestMutation extends ApplicationMutation
class LazyApplicationMutationSpec extends AnyFlatSpec with Matchers {
  it should "preserve authorized mutation envelopes through lazy startup" in {
    val capability = ControlCapability.random()
    implicit val system = ActorSystem("lazy-auth", ControlCapability.runtimeConfig(
      TestUtil.DEFAULT_CONFIG, 1, capability))
    try {
      val target = TestProbe()
      val targetProps: Props = target
      val client = TestProbe()
      val actor = system.actorOf(Props(new LazyStartAppMaster(targetProps)))
      val mutation = ControlRequest(capability, Some(1), TestMutation)
      client.send(actor, mutation)
      actor ! StartAppMaster
      target.expectMsg(mutation)
      Seq[Any](TestMutation, ControlRequest(ControlCapability.random(), Some(1), TestMutation),
        ControlRequest(capability, Some(2), TestMutation)).foreach { denied =>
        client.send(actor, denied)
        client.expectMsgType[Status.Failure]
      }
      target.expectNoMessage(100.millis)
    } finally Await.result(system.terminate(), 10.seconds)
  }
}
