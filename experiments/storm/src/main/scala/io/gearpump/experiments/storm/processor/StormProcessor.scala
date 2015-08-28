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

import java.util.concurrent.TimeUnit
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}

import akka.actor.Actor.Receive
import akka.actor.Cancellable
import backtype.storm.generated.Bolt
import backtype.storm.task._
import backtype.storm.tuple.{Fields, TupleImpl}
import backtype.storm.utils.Utils
import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.experiments.storm.util._
import io.gearpump.streaming.task.{StartTime, Task, TaskContext}

import scala.concurrent.duration.FiniteDuration

object StormProcessor {
  val SYSTEM_TASK_ID = -1
  val SYSTEM_COMPONENT_ID = "__system"
  val SYSTEM_STREAM_ID = "__tick"
  val SYSTEM_COMPONENT_OUTPUT_FIELDS = "rate_secs"
  val TICK_TUPLE_FREQ_SECS = "topology.tick.tuple.freq.secs"
}

private[storm] class StormProcessor(taskContext : TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {
  import StormUtil._
  import io.gearpump.experiments.storm.processor.StormProcessor._

  private val topology = getTopology(conf)
  private val pid = taskContext.taskId.processorId
  private val boltId = conf.getString(GraphBuilder.COMPONENT_ID).getOrElse(
    throw new RuntimeException(s"Storm bolt id not found for processor $pid"))
  private val boltSpec = conf.getValue[Bolt](GraphBuilder.COMPONENT_SPEC).getOrElse(
    throw new RuntimeException(s"Storm bolt spec not found for processor $pid"))
  private val bolt = Utils.getSetComponentObject(boltSpec.get_bolt_object()).asInstanceOf[IBolt]
  private val stormConfig = getStormConfig(conf, boltSpec.get_common())
  private val topologyContextBuilder = TopologyContextBuilder(topology, stormConfig,
    multiLang = bolt.isInstanceOf[ShellBolt])
  private val outputCollector = new StormOutputCollector(taskContext, pid, boltId)
  private var scheduler: Cancellable = null


  override def onStart(startTime: StartTime): Unit = {
    Option(stormConfig.get(TICK_TUPLE_FREQ_SECS)).foreach { freq =>
      val freq = stormConfig.get(TICK_TUPLE_FREQ_SECS).asInstanceOf[java.lang.Long]
      val values = new JArrayList[Object](1)
      values.add(freq)
      val tickTuple = new TupleImpl(getTickTupleContext, values, SYSTEM_TASK_ID, SYSTEM_STREAM_ID, null)
      scheduler = taskContext.schedule(new FiniteDuration(freq, TimeUnit.SECONDS),
        new FiniteDuration(freq, TimeUnit.SECONDS))({
        self ! tickTuple
      })
    }

    val delegate = new StormBoltOutputCollector(outputCollector)
    val topologyContext = topologyContextBuilder.buildContext(pid, boltId)
    bolt.prepare(stormConfig, topologyContext, new OutputCollector(delegate))
  }

  override def onNext(msg: Message): Unit = {
    val stormTuple = msg.msg.asInstanceOf[StormTuple]
    outputCollector.setTimestamp(msg.timestamp)
    bolt.execute(stormTuple.toTuple(topologyContextBuilder))
  }

  override def onStop(): Unit = {
    if (scheduler != null) {
      scheduler.cancel()
    }
  }

  override def receiveUnManagedMessage: Receive = {
    case tickTuple: TupleImpl =>
      bolt.execute(tickTuple)
    case msg =>
      LOG.error("Failed! Received unknown message " + "taskId: " + taskContext.taskId + ", " + msg.toString)
  }

  private def getTickTupleContext: GeneralTopologyContext = {
    val taskToComponent = new JHashMap[Integer, String]
    taskToComponent.put(SYSTEM_TASK_ID, SYSTEM_COMPONENT_ID)
    val componentToSortedTasks = new JHashMap[String, JList[Integer]]
    val tasks = new JArrayList[Integer](1)
    tasks.add(SYSTEM_TASK_ID)
    componentToSortedTasks.put(SYSTEM_COMPONENT_ID, tasks)
    val streamToFields = new JHashMap[String, Fields]
    streamToFields.put(SYSTEM_STREAM_ID, new Fields(SYSTEM_COMPONENT_OUTPUT_FIELDS))
    val componentToStreamToFields = new JHashMap[String, JMap[String, Fields]]
    componentToStreamToFields.put(SYSTEM_COMPONENT_ID, streamToFields)
    new GeneralTopologyContext(topology, stormConfig, taskToComponent, componentToSortedTasks,
      componentToStreamToFields, null)
  }
}
