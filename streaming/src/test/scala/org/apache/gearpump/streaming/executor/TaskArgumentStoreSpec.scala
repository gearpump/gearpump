/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.streaming.executor

import org.apache.gearpump.streaming.executor.TaskLauncher.TaskArgument
import org.scalatest._
import org.apache.gearpump.streaming.executor.Executor.{TaskArgumentStore}
import scala.language.postfixOps
import org.apache.gearpump.streaming.task.TaskId

class TaskArgumentStoreSpec  extends FlatSpec with Matchers with BeforeAndAfterEach {
  it should "retain all history of taskArgument" in {
    val version0 = TaskArgument(0, null, null)
    val version2 = version0.copy(dagVersion = 2)
    val store = new TaskArgumentStore
    val task = TaskId(0, 0)
    store.add(task, version0)
    store.add(task, version2)

    // we should return a version which is same or older than expected version
    assert(store.get(dagVersion = 1, task) == Some(version0))
    assert(store.get(dagVersion = 0, task) == Some(version0))
    assert(store.get(dagVersion = 2, task) == Some(version2))

    store.removeObsoleteVersion
    assert(store.get(dagVersion = 1, task) == None)
    assert(store.get(dagVersion = 0, task) == None)
    assert(store.get(dagVersion = 2, task) == Some(version2))
  }

}
