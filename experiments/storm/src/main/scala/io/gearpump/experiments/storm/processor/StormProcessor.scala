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

package io.gearpump.experiments.storm.processor

import backtype.storm.generated.Bolt
import backtype.storm.task.{IBolt, OutputCollector, ShellBolt}
import backtype.storm.utils.Utils
import io.gearpump.experiments.storm.util._
import io.gearpump.streaming.task.{StartTime, Task, TaskContext}
import io.gearpump.Message
import io.gearpump.cluster.UserConfig

private[storm] class StormProcessor(taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {
  import StormUtil._

  private val topology = getTopology(conf)
  private val stormConfig = getStormConfig(conf)
  private val pid = taskContext.taskId.processorId
  private val boltId = conf.getString(GraphBuilder.COMPONENT_ID).getOrElse(
    throw new RuntimeException(s"Storm bolt id not found for processor $pid"))
  private val boltSpec = conf.getValue[Bolt](GraphBuilder.COMPONENT_SPEC).getOrElse(
    throw new RuntimeException(s"Storm bolt spec not found for processor $pid"))
  private val bolt = Utils.getSetComponentObject(boltSpec.get_bolt_object()).asInstanceOf[IBolt]
  private val topologyContextBuilder = TopologyContextBuilder(topology, stormConfig,
    multiLang = bolt.isInstanceOf[ShellBolt])
  private val outputCollector = new StormOutputCollector(taskContext, pid, boltId)

  override def onStart(startTime: StartTime): Unit = {
    val topologyContext = topologyContextBuilder.buildContext(pid, boltId)
    val delegate = new StormBoltOutputCollector(outputCollector)
    bolt.prepare(stormConfig, topologyContext, new OutputCollector(delegate))
  }

  override def onNext(msg: Message): Unit = {
    val stormTuple = msg.msg.asInstanceOf[StormTuple]
    outputCollector.setTimestamp(msg.timestamp)
    bolt.execute(stormTuple.toTuple(topologyContextBuilder))
  }

}
