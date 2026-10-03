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
import io.gearpump.security._
import io.gearpump.cluster.{ExecutorContext, TestUtil}
import io.gearpump.cluster.appmaster.WorkerInfo
import io.gearpump.cluster.scheduler.Resource
import io.gearpump.cluster.worker.WorkerId
import io.gearpump.examples.distributedshell.DistShellAppMaster.{ShellCommand, ShellCommandResult}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.sys.process._
import scala.util.{Failure, Success, Try}

class ShellExecutorSpec extends AnyWordSpec with Matchers {

  "ShellExecutor" should {
    "execute the shell command and return the result" in {
      val executorId = 1
      val workerId = WorkerId(2, 0L)
      val appId = 0
      val appName = "app"
      val resource = Resource(1)
      val capability = ControlCapability.random()
      implicit val system = ActorSystem("ShellExecutor", ControlCapability.runtimeConfig(TestUtil.DEFAULT_CONFIG, appId, capability))
      val mockMaster = TestProbe()(system)
      val worker = TestProbe()
      val workerInfo = WorkerInfo(workerId, worker.ref)
      val executorContext = ExecutorContext(executorId, workerInfo, appId, appName,
        mockMaster.ref, resource)
      val executor = system.actorOf(Props(classOf[ShellExecutor], executorContext))

      val process = Try(s"ls /".!!)
      val result = process match {
        case Success(msg) => msg
        case Failure(ex) => ex.getMessage
      }
      val marker = java.nio.file.Files.createTempDirectory("denied-shell-").resolve("must-not-exist")
      val command = ShellCommand("touch " + marker.toString)
      Seq[Any](command, ControlRequest(ControlCapability.random(), Some(appId), command),
        ControlRequest(capability, Some(appId + 1), command)).foreach { unauthorized =>
        mockMaster.send(executor, unauthorized)
        mockMaster.expectMsgType[Status.Failure]
        java.nio.file.Files.exists(marker) shouldBe false
      }
      java.nio.file.Files.delete(marker.getParent)
      executor.tell(ControlRequest(capability, Some(appId), ShellCommand("ls /")), mockMaster.ref)
      assert(mockMaster.receiveN(1).head.asInstanceOf[ShellCommandResult].equals(
        ShellCommandResult(executorId, result)))

      system.terminate()
      Await.result(system.whenTerminated, Duration.Inf)
    }
  }
}
