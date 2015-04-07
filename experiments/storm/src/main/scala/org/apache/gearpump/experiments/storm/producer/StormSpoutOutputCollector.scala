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

package org.apache.gearpump.experiments.storm.producer

import java.util.{List => JList}

import backtype.storm.spout.ISpoutOutputCollector
import org.apache.gearpump.Message
import org.apache.gearpump.experiments.storm.util.StormTuple
import org.apache.gearpump.streaming.task.TaskContext

import scala.collection.JavaConverters._

private[storm] class StormSpoutOutputCollector(pid: Int, componentId: String, taskContext: TaskContext) extends ISpoutOutputCollector {
  import taskContext.output

  override def emit(streamId: String, values: JList[AnyRef], messageId: scala.Any): JList[Integer] = {
    val message = Message(StormTuple(values.asScala.toList, pid, componentId, streamId))
    output(message)
    null
  }

  override def reportError(throwable: Throwable): Unit = {
    throw throwable
  }

  override def emitDirect(taskId: Int, streamId: String, values: JList[AnyRef], messageId: scala.Any): Unit = {
    throw new RuntimeException("emit direct not supported")
  }
}
