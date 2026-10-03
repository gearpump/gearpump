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

package io.gearpump.streaming.task

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestProbe
import io.gearpump.cluster.TestUtil
import io.gearpump.util.PekkoHelper
import io.gearpump.Message
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.Await
import scala.concurrent.duration._

class TaskSecurityCheckerSpec extends AnyFlatSpec with Matchers {
  it should "bind sessions to authorized predecessor task indices and retire removed sources" in {
    implicit val system = ActorSystem("task-topology", TestUtil.DEFAULT_CONFIG)
    try {
      val self = TestProbe()
      val stranger = TestProbe()
      val target = TaskId(1, 0)
      val source = TaskId(0, 1)
      val identity = PekkoHelper.sessionActorFor(system, 7, TaskId.toLong(source))
      val checker = new TaskActor.SecurityChecker(target, self.ref, system, Map(0 -> 2))
      checker.knownSender(identity) shouldBe false
      checker.handleInitialAckRequest(InitialAckRequest(source, 7), stranger.ref) shouldBe null
      checker.handleInitialAckRequest(InitialAckRequest(TaskId(2, 0), 7), identity) shouldBe null
      checker.handleInitialAckRequest(InitialAckRequest(TaskId(0, 2), 7), identity) shouldBe null
      checker.handleInitialAckRequest(InitialAckRequest(source, 8), identity) shouldBe null
      checker.handleInitialAckRequest(InitialAckRequest(source, 7), identity) should not be null
      checker.knownSender(identity) shouldBe true
      val allowed = Message("allowed")
      checker.checkMessage(allowed, identity) shouldBe Some(allowed)
      val differentSource = PekkoHelper.sessionActorFor(system, 7, TaskId.toLong(TaskId(0, 0)))
      checker.knownSender(differentSource) shouldBe false
      checker.handleInitialAckRequest(InitialAckRequest(TaskId(0, 0), 7), differentSource) shouldBe null
      checker.updateTopology(Map.empty)
      checker.knownSender(identity) shouldBe false
      checker.checkMessage(Message("denied"), identity) shouldBe None
    } finally Await.result(system.terminate(), 10.seconds)
  }
}
