/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.streaming.examples.state.processor

import com.twitter.algebird.{AveragedGroup, AveragedValue}
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.monoid.AlgebirdGroup
import org.apache.gearpump.streaming.serializer.ChillSerializer
import org.apache.gearpump.streaming.state.api.{PersistentState, PersistentTask}
import org.apache.gearpump.streaming.state.impl.{Interval, Window, WindowConfig, WindowState}
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.collection.immutable.TreeMap

object WindowAverageProcessor {
  val LOG: Logger = LogUtil.getLogger(classOf[WindowAverageProcessor])
}

class WindowAverageProcessor(taskContext : TaskContext, conf: UserConfig)
  extends PersistentTask[AveragedValue](taskContext, conf) {

  override def persistentState: PersistentState[AveragedValue] = {
    val group = new AlgebirdGroup(AveragedGroup)
    val serializer = new ChillSerializer[TreeMap[Interval, AveragedValue]]
    val window = new Window(conf.getValue[WindowConfig](WindowConfig.NAME).get)
    new WindowState[AveragedValue](group, serializer, taskContext, window)
  }

  override def processMessage(state: PersistentState[AveragedValue],
                              message: Message): Unit = {
    val value = AveragedValue(message.msg.asInstanceOf[String].toLong)
    state.update(message.timestamp, value)
  }
}
