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

package org.apache.gearpump.experiments.storm.util

import java.util.{ArrayList => JArrayList, Collection => JCollection, List => JList}

import org.apache.gearpump._
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.collection.JavaConversions._

object StormOutputCollector {
  private val EMPTY_LIST = new JArrayList[Integer](0)
  private val LOG: Logger = LogUtil.getLogger(classOf[StormOutputCollector])
}

class StormOutputCollector(context: TaskContext, pid: Int, componentId: String) {
  import org.apache.gearpump.experiments.storm.util.StormOutputCollector._

  private var timestamp: TimeStamp = LatestTime

  def emit(streamId: String, values: JList[AnyRef]): JList[Integer] = {
    context.output(Message(StormTuple(values.toList, pid, componentId, streamId), timestamp))
    EMPTY_LIST
  }

  def emitDirect(taskId: Int, streamId: String, tuple: JList[AnyRef]): Unit = {
    LOG.error("emit direct not supported")
  }

  def setTimestamp(timestamp: TimeStamp): Unit = {
    this.timestamp = timestamp
  }

}
