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
package io.gearpump.examples.distributedshell

import org.apache.pekko.actor.{ActorSystem, Props, Status}
import org.apache.pekko.testkit.TestProbe
import io.gearpump.cluster._
import io.gearpump.cluster.scheduler.Resource
import io.gearpump.security._
import io.gearpump.examples.distributedshell.DistShellAppMaster.ShellCommand
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.Await
import scala.concurrent.duration._

class ShellAuthorizationSpec extends AnyFlatSpec with Matchers {
  it should "reject commands from another app at the public AppMaster" in {
    val capability = ControlCapability.random()
    implicit val system = ActorSystem("shell-auth", ControlCapability.runtimeConfig(
      TestUtil.DEFAULT_CONFIG, 1, capability))
    try {
      val master = TestProbe()
      val client = TestProbe()
      val app = AppDescription("shell", classOf[DistShellAppMaster].getName, UserConfig.empty,
        clusterConfig = system.settings.config)
      val actor = system.actorOf(Props(new DistShellAppMaster(AppMasterContext(1, "owner",
        Resource(1), null, None, master.ref), app)))
      Seq[Any](ShellCommand("echo denied"), ControlRequest(ControlCapability.random(), Some(1),
        ShellCommand("echo denied")), ControlRequest(capability, Some(2),
        ShellCommand("echo denied"))).foreach { command =>
        client.send(actor, command)
        client.expectMsgType[Status.Failure]
      }
      client.send(actor, ControlRequest(capability, Some(1), ShellCommand("echo allowed")))
      client.expectMsg("") // No executors exist; authorized broadcast completes without a command.
    } finally Await.result(system.terminate(), 10.seconds)
  }
}
