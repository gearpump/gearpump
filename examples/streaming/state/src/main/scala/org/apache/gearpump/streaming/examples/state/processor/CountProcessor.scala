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

package org.apache.gearpump.streaming.examples.state.processor

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.monoid.AlgebirdMonoid
import org.apache.gearpump.streaming.serializer.ChillSerializer
import org.apache.gearpump.streaming.state.api.{PersistentState, PersistentTask}
import org.apache.gearpump.streaming.state.impl.NonWindowState
import org.apache.gearpump.streaming.task.TaskContext

class CountProcessor(taskContext: TaskContext, conf: UserConfig)
  extends PersistentTask[Int](taskContext, conf) {

  private val serializer = new ChillSerializer[Int]

  override def persistentState: PersistentState[Int] = {
    import com.twitter.algebird.Monoid.intMonoid
    new NonWindowState[Int](new AlgebirdMonoid(intMonoid), serializer)
  }

  override def processMessage(state: PersistentState[Int], message: Message): Unit = {
    state.update(message.timestamp.toEpochMilli, 1)
    state.get.foreach(s => taskContext.output(Message(serializer.serialize(s), message.timestamp)))
  }
}

