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
package gearpump.streaming.appmaster

import gearpump.streaming.executor.ExecutorRestartPolicy
import gearpump.streaming.task.TaskId
import org.scalatest.{Matchers, WordSpec}
import scala.concurrent.duration._

class ExecutorRestartPolicySpec extends WordSpec with Matchers {

  "ExecutorRestartPolicy" should {
    "decide whether to restart the executor" in {
      val executorId1 = 1
      val executorId2 = 2
      val taskId = TaskId(0, 0)
      val executorSupervisor = new ExecutorRestartPolicy(maxNrOfRetries = 3, withinTimeRange = 1 seconds)
      executorSupervisor.addTaskToExecutor(executorId1, taskId)
      assert(executorSupervisor.allowRestartExecutor(executorId1))
      assert(executorSupervisor.allowRestartExecutor(executorId1))
      executorSupervisor.addTaskToExecutor(executorId2, taskId)
      assert(executorSupervisor.allowRestartExecutor(executorId2))
      assert(!executorSupervisor.allowRestartExecutor(executorId2))
      Thread.sleep(1000)
      assert(executorSupervisor.allowRestartExecutor(executorId2))
    }
  }
}
