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
import backtype.storm.generated.{GlobalStreamId, Bolt}
import backtype.storm.tuple.Tuple
import backtype.storm.utils.Utils
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.{TestUtil, UserConfig}
import org.apache.gearpump.experiments.storm.util.{TopologyContextBuilder, TopologyUtil, GraphBuilder}
import org.apache.gearpump.experiments.storm.util.GraphBuilder._
import org.apache.gearpump.streaming._
import org.apache.gearpump.streaming.task.{StartTime, TaskId}
import org.json.simple.JSONValue
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class StormProcessorSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {
  import org.apache.gearpump.experiments.storm.util.StormUtil._

  property("StormProcessor should work") {
    implicit val system = ActorSystem("test",  TestUtil.DEFAULT_CONFIG)
    val topology = TopologyUtil.getTestTopology
    val componentToStreamFields = getComponentToStreamFields(topology)
    val graphBuilder = new GraphBuilder()
    val processorGraph = graphBuilder.build(topology)
    val processors = DAG(processorGraph.mapVertex(Processor.ProcessorToProcessorDescription(_))).processors
    val stormConfig = Utils.readStormConfig()
    val userConfig = UserConfig.empty
      .withValue(TOPOLOGY, topology)
      .withValue(STORM_CONFIG, JSONValue.toJSONString(stormConfig))
    val fieldGen = Gen.alphaStr
    val topologyContextBuilder = new TopologyContextBuilder(topology, stormConfig)

    val bolts = topology.get_bolts()
    forAll(fieldGen) { (field: String) =>
      processors foreach { case (pid, procDesc) =>
        val conf = procDesc.taskConf
        val cid = conf.getString(COMPONENT_ID).getOrElse(
          fail(s"component id not found for processor $pid")
        )
        if (bolts.containsKey(cid)) {
          val bolt = conf.getValue[Bolt](COMPONENT_SPEC).getOrElse(
            fail(s"bolt not found for processor $pid")
          )
          bolt shouldBe bolts.get(cid)
          bolt.get_common().get_inputs() foreach { case (streamId, grouping) =>
            val (spid, scid) = findSourceTaskId(processors, streamId, pid)
            val taskContext = MockUtil.mockTaskContext
            when(taskContext.taskId).thenReturn(TaskId(pid, 0))
            val stormProcessor = new StormProcessor(taskContext, procDesc.taskConf.withConfig(userConfig))
            stormProcessor.onStart(StartTime(0))
            val values: List[AnyRef] = List.fill(componentToStreamFields.get(scid).size)(field)
            val topologyContext = topologyContextBuilder.buildContext(spid, scid)
            val tuple = topologyContextBuilder.buildTuple(values.asJava, topologyContext, spid, scid, streamId.get_streamId())
            stormProcessor.onNext(Message(tuple))
            verify(taskContext).output(MockUtil.argMatch[Message](msg => msg.msg.isInstanceOf[Tuple]))
          }
        }
      }
    }
  }

  private def findSourceTaskId(processors: Map[ProcessorId, ProcessorDescription], streamId: GlobalStreamId, pid: ProcessorId): (ProcessorId, String) = {
    val (sourceTaskId, sourceProcDesc) = processors.find { case (id, desc) =>
      desc.taskConf.getString(COMPONENT_ID).getOrElse(
        fail(s"component id not found for processor $id")
      ) ==  streamId.get_componentId()
    }.getOrElse(fail(s"source task not found for processor $pid"))
    sourceTaskId -> sourceProcDesc.taskConf.getString(COMPONENT_ID)
      .getOrElse(fail(s"component id not found for processor $sourceTaskId"))
  }
}
