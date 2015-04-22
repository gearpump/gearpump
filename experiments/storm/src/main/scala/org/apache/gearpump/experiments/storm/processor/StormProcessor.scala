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

import java.util.concurrent.TimeUnit

import akka.actor.Cancellable
import backtype.storm.generated.Bolt
import backtype.storm.task.{ShellBolt, IBolt, OutputCollector}
import backtype.storm.utils.Utils
import java.util.{List => JList}
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.experiments.storm.util.{TopologyContextBuilder, GraphBuilder, StormTuple}
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}

import scala.concurrent.duration.FiniteDuration
import scala.collection.JavaConversions._

private[storm] class StormProcessor (taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {
  import org.apache.gearpump.experiments.storm.util.StormUtil._

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
  private var count = 0
  private var snapShotTime : Long = System.currentTimeMillis()
  private var snapShotWordCount : Long = 0

  private var scheduler : Cancellable = null

  override def onStart(startTime: StartTime): Unit = {
    val topologyContext = topologyContextBuilder.buildContext(pid, boltId)
    val outputFn = (streamId: String, tuple: JList[AnyRef]) => {
      taskContext.output(Message(StormTuple(tuple.toList, pid, boltId, streamId)))
    }
    val delegate = new StormBoltOutputCollector(outputFn)
    bolt.prepare(stormConfig, topologyContext, new OutputCollector(delegate))
    scheduler = taskContext.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
      new FiniteDuration(5, TimeUnit.SECONDS))(reportWordCount)
  }

  override def onNext(msg: Message): Unit = {
    val stormTuple = msg.msg.asInstanceOf[StormTuple]
    bolt.execute(stormTuple.toTuple(topologyContextBuilder))
    count += 1
  }

  private def reportWordCount() : Unit = {
    val current : Long = System.currentTimeMillis()
    LOG.info(s"Task ${taskContext.taskId} Throughput: ${(count - snapShotWordCount, (current - snapShotTime) / 1000)} (words, second)")
    snapShotWordCount = count
    snapShotTime = current
  }

}
