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

package io.gearpump.streaming.examples.state.processor

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.monoid.AlgebirdMonoid
import io.gearpump.streaming.serializer.ChillSerializer
import io.gearpump.streaming.state.api.{PersistentState, PersistentTask}
import io.gearpump.streaming.state.impl.NonWindowState
import io.gearpump.streaming.task.TaskContext

class CountProcessor(taskContext: TaskContext, conf: UserConfig)
  extends PersistentTask[Int](taskContext, conf) {

  override def persistentState: PersistentState[Int] = {
    import com.twitter.algebird.Monoid.intMonoid
    new NonWindowState[Int](new AlgebirdMonoid(intMonoid), new ChillSerializer[Int])
  }

  override def processMessage(state: PersistentState[Int], message: Message): Unit = {
    state.update(message.timestamp, 1)
  }
}


