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

import backtype.storm.spout.{ISpout, SpoutOutputCollector}
import backtype.storm.utils.Utils
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}

private[storm] class StormProducer(taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {
  import org.apache.gearpump.experiments.storm.util.StormUtil._

  private val topology = getTopology(conf)
  private val processorToComponent = getProcessorToComponent(conf)
  private val stormConfig = getStormConfig(conf)
  private val pid = taskContext.taskId.processorId
  private val topologyContext = getTopologyContext(topology, stormConfig, processorToComponent, pid)
  private val spouts = topology.get_spouts()

  private val spoutSpec = spouts.get(processorToComponent.getOrElse(pid,
    throw new RuntimeException(s"processor $pid has no mapping component")))
  private val spout = Utils.getSetComponentObject(spoutSpec.get_spout_object()).asInstanceOf[ISpout]

  override def onStart(startTime: StartTime): Unit = {
    val streamId = spoutSpec.get_common()
    val delegate = new StormSpoutOutputCollector(pid, taskContext)
    spout.open(stormConfig, topologyContext, new SpoutOutputCollector(delegate))
    self ! Message("start")
  }

  override def onNext(msg: Message): Unit = {
    spout.nextTuple()
    self ! Message("Continue")
  }
}
