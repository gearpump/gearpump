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

package io.gearpump.experiments.storm.producer

import backtype.storm.generated.{StormTopology, SpoutSpec}
import backtype.storm.spout.{ShellSpout, ISpout, SpoutOutputCollector}
import backtype.storm.utils.Utils
import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.experiments.storm.util.{GraphBuilder, StormOutputCollector, StormUtil}
import io.gearpump.streaming.task.{StartTime, Task, TaskContext}

import scala.collection.JavaConverters._

private[storm] class StormProducer(taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {
  import StormUtil._

  private val topology = conf.getValue[StormTopology](TOPOLOGY).get
  private val spoutId = conf.getString(GraphBuilder.COMPONENT_ID).get
  private val spoutSpec = conf.getValue[SpoutSpec](GraphBuilder.COMPONENT_SPEC).get
  private val spout = Utils.getSetComponentObject(spoutSpec.get_spout_object()).asInstanceOf[ISpout]
  private val stormConfig = getStormConfig(conf, spoutSpec.get_common())
  private val taskToComponent = conf.getValue[Map[Integer, String]](TASK_TO_COMPONENT).get
  private val componentToSortedTasks = getComponentToSortedTasks(taskToComponent)
  private val componentToStreamFields = getComponentToStreamFields(topology)
  private val stormTaskId = getStormTaskId(spoutId, componentToSortedTasks, taskContext.taskId)
  private var collector: StormOutputCollector = null

  override def onStart(startTime: StartTime): Unit = {
    val topologyContext = buildTopologyContext(topology, stormConfig, taskToComponent.asJava,
      componentToSortedTasks, componentToStreamFields, spoutId, stormTaskId, spout.isInstanceOf[ShellSpout])
    val targets = topologyContext.getTargets(spoutId)
    collector = new StormOutputCollector(taskContext, topology, spoutId, spoutSpec.get_common(), stormTaskId,
      taskToComponent, componentToSortedTasks, targets)
    val delegate = new StormSpoutOutputCollector(collector)
    spout.open(stormConfig, topologyContext, new SpoutOutputCollector(delegate))
    self ! Message("start")
  }

  override def onNext(msg: Message): Unit = {
    collector.setTimestamp(System.currentTimeMillis())
    spout.nextTuple()
    self ! Message("Continue")
  }
}
