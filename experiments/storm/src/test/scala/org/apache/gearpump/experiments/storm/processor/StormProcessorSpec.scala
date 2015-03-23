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

import akka.actor.ActorSystem
import backtype.storm.utils.Utils
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.experiments.storm.util.{StormTuple, GraphBuilder, TopologyUtil}
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.task.{StartTime, TaskId}
import org.json.simple.JSONValue
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}
import scala.collection.JavaConversions._

class StormProcessorSpec extends PropSpec with PropertyChecks with Matchers {
  import org.apache.gearpump.experiments.storm.util.StormUtil._

  property("StormProcessor should work") {
    implicit val system = ActorSystem("test")
    val topology = TopologyUtil.getTestTopology
    val graphBuilder = new GraphBuilder(topology)
    graphBuilder.build()
    val processorToComponent = graphBuilder.getProcessorToComponent
    val componentToProcessor = graphBuilder.getComponentToProcessor
    val stormConfig = Utils.readStormConfig()
    val userConfig = UserConfig.empty
      .withValue(TOPOLOGY, topology)
      .withValue(PROCESSOR_TO_COMPONENT, processorToComponent.toList)
      .withValue(STORM_CONFIG, JSONValue.toJSONString(stormConfig))
    val fieldGen = Gen.alphaStr
    forAll(fieldGen) { (field: String) =>
      val values: List[AnyRef] = List(field)
      topology.get_bolts foreach { case (id, bolt) =>
        bolt.get_common().get_inputs() foreach { case(streamId, _) =>
          componentToProcessor.get(streamId.get_componentId()) foreach { sourceTaskId =>
            componentToProcessor.get(id).foreach { pid =>
              val taskContext = MockUtil.mockTaskContext
              when(taskContext.taskId).thenReturn(TaskId(pid, 0))
              val stormProcessor = new StormProcessor(taskContext, userConfig)
              stormProcessor.onStart(StartTime(0))
              stormProcessor.onNext(Message(StormTuple(values, sourceTaskId, Utils.DEFAULT_STREAM_ID)))
              verify(taskContext).output(anyObject())
            }
          }
        }
      }
    }
  }
}
