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

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import backtype.storm.utils.Utils
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.experiments.storm.util.{GraphBuilder, TopologyUtil}
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.task.{StartTime, TaskId}
import org.json.simple.JSONValue
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}
import scala.collection.JavaConversions._

class StormProducerSpec extends PropSpec with PropertyChecks with Matchers {
  import org.apache.gearpump.experiments.storm.util.StormUtil._

  property("StormProducer should work") {
    implicit val system = ActorSystem("test")
    val topology = TopologyUtil.getTestTopology
    val graphBuilder = new GraphBuilder(topology)
    graphBuilder.build()
    val processorToComponent = graphBuilder.getProcessorToComponent
    val stormConfig = Utils.readStormConfig()
    val userConfig = UserConfig.empty
      .withValue(TOPOLOGY, topology)
      .withValue(PROCESSOR_TO_COMPONENT, processorToComponent.toList)
      .withValue(STORM_CONFIG, JSONValue.toJSONString(stormConfig))

    val mockTaskActor = TestProbe()

    topology.get_spouts foreach { case (id, _) =>
      processorToComponent.find(_._2 == id).foreach { case (pid, _) =>
        val taskContext = MockUtil.mockTaskContext
        when(taskContext.self).thenReturn(mockTaskActor.ref)
        when(taskContext.taskId).thenReturn(TaskId(pid, 0))
        val stormProducer = new StormProducer(taskContext, userConfig)
        stormProducer.onStart(StartTime(0))
        mockTaskActor.expectMsgType[Message]
        stormProducer.onNext(Message("Next"))
        mockTaskActor.expectMsgType[Message]
        verify(taskContext).output(anyObject())
      }
    }
  }
}
