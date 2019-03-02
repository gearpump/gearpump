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
package io.gearpump.streaming.executor

import akka.actor.{Actor, ActorSystem}
import akka.testkit.TestProbe
import io.gearpump.cluster.{TestUtil, UserConfig}
import io.gearpump.serializer.SerializationFramework
import io.gearpump.streaming.ProcessorDescription
import io.gearpump.streaming.executor.TaskLauncher.TaskArgument
import io.gearpump.streaming.executor.TaskLauncherSpec.{MockTask, MockTaskActor}
import io.gearpump.streaming.task.{Task, TaskContext, TaskContextData, TaskId, TaskWrapper}
import org.scalatest._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class TaskLauncherSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val appId = 0
  val executorId = 0
  var appMaster: TestProbe = null
  implicit var system: ActorSystem = null
  val userConf = UserConfig.empty

  override def beforeAll(): Unit = {
    system = ActorSystem("TaskLauncherSpec", TestUtil.DEFAULT_CONFIG)
    appMaster = TestProbe()
  }

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
  }

  it should "able to launch tasks" in {
    val launcher = new TaskLauncher(appId, "app", executorId, appMaster.ref,
      userConf, classOf[MockTaskActor])
    val taskIds = List(TaskId(0, 0), TaskId(0, 1))
    val processor = ProcessorDescription(id = 0, taskClass = classOf[MockTask].getName,
      parallelism = 2)
    val argument = TaskArgument(0, processor, null)

    val tasks = launcher.launch(taskIds, argument, system, null,
      "gearpump.shared-thread-pool-dispatcher")
    tasks.keys.toSet shouldBe taskIds.toSet
  }
}

object TaskLauncherSpec {
  class MockTaskActor(
      val taskId: TaskId,
      val taskContextData : TaskContextData,
      val task: TaskWrapper,
      val serializer: SerializationFramework) extends Actor {
    def receive: Receive = null
  }

  class MockTask(taskContext: TaskContext, userConf: UserConfig)
    extends Task(taskContext, userConf) {
  }
}
