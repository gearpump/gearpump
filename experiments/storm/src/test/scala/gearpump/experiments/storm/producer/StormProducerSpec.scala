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

package gearpump.experiments.storm.producer

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import backtype.storm.generated.{SpoutSpec}
import backtype.storm.utils.Utils
import gearpump.Message
import gearpump.experiments.storm.util.{TopologyUtil, StormUtil, GraphBuilder}
import gearpump.streaming.{Processor, DAG, MockUtil}
import gearpump.streaming.task.{TaskId, StartTime}
import gearpump.cluster.{TestUtil, UserConfig}
import GraphBuilder._
import org.json.simple.JSONValue
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}
import scala.collection.JavaConversions._

class StormProducerSpec extends PropSpec with PropertyChecks with Matchers {
  import StormUtil._

  property("StormProducer should work") {
    implicit val system = ActorSystem("test",  TestUtil.DEFAULT_CONFIG)
    val topology = TopologyUtil.getTestTopology
    val graphBuilder = new GraphBuilder
    val processorGraph = graphBuilder.build(topology)
    val processors = DAG(processorGraph.mapVertex(Processor.ProcessorToProcessorDescription(_))).processors
    val stormConfig = Utils.readStormConfig()
    val userConfig = UserConfig.empty
      .withValue(TOPOLOGY, topology)
      .withValue(STORM_CONFIG, JSONValue.toJSONString(stormConfig))

    val mockTaskActor = TestProbe()

    val spouts = topology.get_spouts()
    processors.foreach { case (pid, procDesc) =>
      val conf = procDesc.taskConf
      val cid = conf.getString(COMPONENT_ID).getOrElse(
        fail(s"component id not found for processor $pid")
      )
      if (spouts.containsKey(cid)) {
        val spoutSpec = conf.getValue[SpoutSpec](COMPONENT_SPEC).getOrElse(
          fail(s"spout not found for processor $pid")
        )
        spoutSpec shouldBe spouts.get(cid)

        val taskContext = MockUtil.mockTaskContext
        when(taskContext.self).thenReturn(mockTaskActor.ref)
        when(taskContext.taskId).thenReturn(TaskId(pid, 0))
        val stormProducer = new StormProducer(taskContext, procDesc.taskConf.withConfig(userConfig))
        stormProducer.onStart(StartTime(0))
        mockTaskActor.expectMsgType[Message]
        stormProducer.onNext(Message("Next"))
        mockTaskActor.expectMsgType[Message]
        verify(taskContext).output(anyObject())
      }
    }
  }
}
