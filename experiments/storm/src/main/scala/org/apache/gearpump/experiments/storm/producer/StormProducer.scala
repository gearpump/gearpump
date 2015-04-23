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

import backtype.storm.generated.SpoutSpec
import backtype.storm.spout.{ShellSpout, ISpout, SpoutOutputCollector}
import backtype.storm.utils.Utils
import java.util.{List => JList}
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.experiments.storm.util.{TopologyContextBuilder, GraphBuilder}
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}

private[storm] class StormProducer(taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {
  import org.apache.gearpump.experiments.storm.util.StormUtil._

  private val topology = getTopology(conf)
  private val stormConfig = getStormConfig(conf)
  private val pid = taskContext.taskId.processorId

  private val spoutId = conf.getString(GraphBuilder.COMPONENT_ID)
      .getOrElse(throw new RuntimeException(s"Storm spout id not found for processor $pid"))
  private val spoutSpec = conf.getValue[SpoutSpec](GraphBuilder.COMPONENT_SPEC)
      .getOrElse(throw new RuntimeException(s"Storm spout spec not found for processor $pid"))
  private val spout = Utils.getSetComponentObject(spoutSpec.get_spout_object()).asInstanceOf[ISpout]
  private val topologyContextBuilder = TopologyContextBuilder(topology, stormConfig, multiLang = spout.isInstanceOf[ShellSpout])

  override def onStart(startTime: StartTime): Unit = {
    val topologyContext = topologyContextBuilder.buildContext(pid, spoutId)
    val outputFn = (streamId: String, values: JList[AnyRef]) => {
      val tuple = topologyContextBuilder.buildTuple(values, topologyContext, pid, spoutId, streamId)
      taskContext.output(Message(tuple))
    }
    val delegate = new StormSpoutOutputCollector(outputFn)
    spout.open(stormConfig, topologyContext, new SpoutOutputCollector(delegate))
    self ! Message("start")
  }

  override def onNext(msg: Message): Unit = {
    spout.nextTuple()
    self ! Message("Continue")
  }
}
