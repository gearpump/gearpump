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

package akka.stream.gearpump.task

import akka.stream.gearpump.task.GraphTask.{Index, PortId}
import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.ProcessorId
import io.gearpump.streaming.task.{StartTime, Task, TaskContext, TaskWrapper}


class GraphTask(inputTaskContext : TaskContext, userConf : UserConfig) extends Task(inputTaskContext, userConf) {

  private val context = inputTaskContext.asInstanceOf[TaskWrapper]
  private val outMapping = portsMapping(userConf.getValue[List[ProcessorId]](GraphTask.OUT_PROCESSORS).get)
  private val inMapping = portsMapping(userConf.getValue[List[ProcessorId]](GraphTask.IN_PROCESSORS).get)

  val sizeOfOutPorts = outMapping.keys.size
  val sizeOfInPorts = inMapping.keys.size
  
  private def portsMapping(processors: List[ProcessorId]): Map[PortId, Index] = {
    val portToProcessor = processors.zipWithIndex.map{kv =>
      (kv._2, kv._1)
    }.toMap

    val processorToIndex = processors.sorted.zipWithIndex.toMap

    val portToIndex = portToProcessor.map{kv =>
      val (outlet, processorId) = kv
      val index = processorToIndex(processorId)
      (outlet, index)
    }
    portToIndex
  }

  def output(outletId: Int, msg: Message): Unit = {
    context.output(outMapping(outletId), msg)
  }

  override def onStart(startTime : StartTime) : Unit = {}

  override def onStop() : Unit = {}
}

object GraphTask {
  val OUT_PROCESSORS = "akka.stream.gearpump.task.outprocessors"
  val IN_PROCESSORS = "akka.stream.gearpump.task.inprocessors"

  type PortId = Int
  type Index = Int
}