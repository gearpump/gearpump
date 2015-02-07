/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package org.apache.gearpump.streaming.appmaster

import org.apache.gearpump.streaming.task.{TaskLocations, TaskId}
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.LogUtil
import org.scalatest.{FlatSpec, BeforeAndAfterEach, Matchers, WordSpec}
import org.slf4j.Logger

class TaskRegistrationSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  it should "maintain registered tasks" in {
    val register = new TaskRegistration(appId = 0, totalTaskCount = 3)
    val host1 = HostPort("127.0.0.1:3000")
    val host2 = HostPort("127.0.0.1:3001")
    val task0 = TaskId(0, 0)
    val task1 = TaskId(0, 1)
    val task2 = TaskId(0, 2)
    register.registerTask(task0, host1)
    register.registerTask(task1, host1)
    register.registerTask(task2, host2)

    assert(register.isAllTasksRegistered)
    val TaskLocations(taskLocations) = register.getTaskLocations
    val tasksOnHost1 = taskLocations.get(host1).get
    val tasksOnHost2 = taskLocations.get(host2).get
    assert(tasksOnHost1.contains(task0))
    assert(tasksOnHost1.contains(task1))
    assert(tasksOnHost2.contains(task2))
  }
}
