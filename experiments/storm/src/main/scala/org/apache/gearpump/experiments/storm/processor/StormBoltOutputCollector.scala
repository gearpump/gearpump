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

package org.apache.gearpump.experiments.storm.processor

import java.util.{Collection => JCollection, List => JList}

import backtype.storm.task.IOutputCollector
import backtype.storm.tuple.Tuple
import org.apache.gearpump.Message
import org.apache.gearpump.experiments.storm.util.StormTuple
import org.apache.gearpump.streaming.task.TaskContext

import scala.collection.JavaConversions._

private[storm] class StormBoltOutputCollector(pid: Int, componentId: String, taskContext: TaskContext) extends IOutputCollector {
  import taskContext.output

  override def emit(streamId: String, anchors: JCollection[Tuple], tuple: JList[AnyRef]): JList[Integer] = {
    output(Message(StormTuple(tuple.toList, pid, componentId, streamId)))
    null
  }

  override def emitDirect(i: Int, s: String, collection: JCollection[Tuple], list: JList[AnyRef]): Unit = {
    throw new RuntimeException("emit direct not supported")
  }

  override def fail(tuple: Tuple): Unit = {}

  override def ack(tuple: Tuple): Unit = {}

  override def reportError(throwable: Throwable): Unit = {
    throw throwable
  }
}
