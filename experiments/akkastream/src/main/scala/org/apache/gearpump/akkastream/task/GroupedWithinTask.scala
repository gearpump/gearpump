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

package org.apache.gearpump.akkastream.task

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.TaskContext

import scala.collection.immutable.VectorBuilder
import scala.concurrent.duration.FiniteDuration

class GroupedWithinTask[T](context: TaskContext, userConf : UserConfig)
  extends GraphTask(context, userConf) {

  case object GroupedWithinTrigger
  val buf: VectorBuilder[T] = new VectorBuilder
  val timeWindow = userConf.getValue[FiniteDuration](GroupedWithinTask.TIME_WINDOW)
  val batchSize = userConf.getInt(GroupedWithinTask.BATCH_SIZE)

  override def onNext(msg: Message) : Unit = {

  }
}

object GroupedWithinTask {
  val BATCH_SIZE = "BATCH_SIZE"
  val TIME_WINDOW = "TIME_WINDOW"
}
