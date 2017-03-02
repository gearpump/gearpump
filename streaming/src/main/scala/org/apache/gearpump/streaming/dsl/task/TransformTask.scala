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
import org.apache.gearpump.streaming.dsl.task.TransformTask.Transform
import org.apache.gearpump.streaming.task.{Task, TaskContext}

object TransformTask {

  class Transform[IN, OUT](taskContext: TaskContext,
      operator: Option[FunctionRunner[IN, OUT]],
      private var buffer: Vector[Message] = Vector.empty[Message]) {

    def onStart(startTime: Instant): Unit = {
      operator.foreach(_.setup())
    }

    def onNext(msg: Message): Unit = {
      buffer +:= msg
    }

    def onStop(): Unit = {
      operator.foreach(_.teardown())
    }

    def onWatermarkProgress(watermark: Instant): Unit = {
      val watermarkTime = watermark.toEpochMilli
      var nextBuffer = Vector.empty[Message]
      val processor = operator.map(FunctionRunner.withEmitFn(_,
        (out: OUT) => taskContext.output(Message(out, watermarkTime))))
      buffer.foreach { case message@Message(in, time) =>
        if (time <= watermarkTime) {
          processor match {
            case Some(p) =>
              // .toList forces eager evaluation
              p.process(in.asInstanceOf[IN]).toList
            case None =>
              taskContext.output(Message(in, watermarkTime))
          }
        } else {
          nextBuffer +:= message
        }
      }
      // .toList forces eager evaluation
      processor.map(_.finish())
      buffer = nextBuffer
    }
  }

}

class TransformTask[IN, OUT](transform: Transform[IN, OUT],
    taskContext: TaskContext, userConf: UserConfig) extends Task(taskContext, userConf) {

  def this(taskContext: TaskContext, userConf: UserConfig) = {
    this(new Transform(taskContext, userConf.getValue[FunctionRunner[IN, OUT]](
      GEARPUMP_STREAMING_OPERATOR)(taskContext.system)), taskContext, userConf)
  }

  override def onStart(startTime: Instant): Unit = {
    transform.onStart(startTime)
  }

  override def onNext(msg: Message): Unit = {
    transform.onNext(msg)
  }

  override def onStop(): Unit = {
    transform.onStop()
  }

  override def onWatermarkProgress(watermark: Instant): Unit = {
    transform.onWatermarkProgress(watermark)
  }
}
