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

package io.gearpump.streaming.appmaster

import io.gearpump.cluster.scheduler.Resource
import io.gearpump.streaming.appmaster.TaskRegistry.{Accept, Reject, TaskLocation, TaskLocations}
import io.gearpump.streaming.task.TaskId
import io.gearpump.transport.HostPort
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
class TaskRegistrySpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  it should "maintain registered tasks" in {
    val task0 = TaskId(0, 0)
    val task1 = TaskId(0, 1)
    val task2 = TaskId(0, 2)

    val register = new TaskRegistry(expectedTasks = List(task0, task1, task2))
    val host1 = HostPort("127.0.0.1:3000")
    val host2 = HostPort("127.0.0.1:3001")

    val executorId = 0
    assert(Accept == register.registerTask(task0, TaskLocation(executorId, host1)))
    assert(Accept == register.registerTask(task1, TaskLocation(executorId, host1)))
    assert(Accept == register.registerTask(task2, TaskLocation(executorId, host2)))

    assert(Reject == register.registerTask(TaskId(100, 0), TaskLocation(executorId, host2)))

    assert(register.isAllTasksRegistered)
    val TaskLocations(taskLocations) = register.getTaskLocations
    val tasksOnHost1 = taskLocations.get(host1).get
    val tasksOnHost2 = taskLocations.get(host2).get
    assert(tasksOnHost1.contains(task0))
    assert(tasksOnHost1.contains(task1))
    assert(tasksOnHost2.contains(task2))

    assert(register.getExecutorId(task0) == Some(executorId))
    assert(register.isTaskRegisteredForExecutor(executorId))

    register.processorExecutors(0) shouldBe Map(
      executorId -> List(task0, task1, task2)
    )

    register.usedResource.resources shouldBe Map(
      executorId -> Resource(3)
    )
  }
}
