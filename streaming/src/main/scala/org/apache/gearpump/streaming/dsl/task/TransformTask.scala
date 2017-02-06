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
package org.apache.gearpump.streaming.dsl.task

import java.time.Instant

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.Constants._
import org.apache.gearpump.streaming.dsl.plan.functions.FunctionRunner
import org.apache.gearpump.streaming.task.{Task, TaskContext}

class TransformTask[IN, OUT](operator: Option[FunctionRunner[IN, OUT]],
    taskContext: TaskContext, userConf: UserConfig) extends Task(taskContext, userConf) {

  def this(taskContext: TaskContext, userConf: UserConfig) = {
    this(userConf.getValue[FunctionRunner[IN, OUT]](
      GEARPUMP_STREAMING_OPERATOR)(taskContext.system), taskContext, userConf)
  }

  override def onStart(startTime: Instant): Unit = {
    operator.foreach(_.setup())
  }

  override def onNext(msg: Message): Unit = {
    val time = msg.timestamp

    operator match {
      case Some(op) =>
        op.process(msg.msg.asInstanceOf[IN]).foreach { msg =>
          taskContext.output(new Message(msg, time))
        }
      case None =>
        taskContext.output(new Message(msg.msg, time))
    }
  }

  override def onStop(): Unit = {
    operator.foreach(_.teardown())
  }
}
