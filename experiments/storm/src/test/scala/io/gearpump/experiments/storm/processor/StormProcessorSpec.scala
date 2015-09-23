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

import akka.actor.ActorSystem
import backtype.storm.generated.{Bolt, GlobalStreamId}
import backtype.storm.utils.Utils
import io.gearpump.Message
import io.gearpump.cluster.{TestUtil, UserConfig}
import io.gearpump.experiments.storm.util.GraphBuilder._
import io.gearpump.experiments.storm.util.{GraphBuilder, StormTuple, StormUtil, TopologyUtil}
import io.gearpump.partitioner.{PartitionerDescription, PartitionerObject}
import io.gearpump.streaming._
import io.gearpump.streaming.task.{StartTime, TaskId}
import org.json.simple.JSONValue
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

import scala.collection.JavaConversions._

class StormProcessorSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {
  import StormUtil._

  property("StormProcessor should work") {
    implicit val system = ActorSystem("test",  TestUtil.DEFAULT_CONFIG)
    val topology = TopologyUtil.getTestTopology
    val (processorGraph, taskToComponent) = GraphBuilder.build(topology)
    val componentToStreamFields = getComponentToStreamFields(topology)

    var processorIdIndex = 0
    val processorDescriptionGraph = processorGraph.mapVertex { processor =>
      val description = Processor.ProcessorToProcessorDescription(processorIdIndex, processor)
      processorIdIndex += 1
      description
    }.mapEdge { (node1, edge, node2) =>
      PartitionerDescription(new PartitionerObject(edge))
    }
    val dag = DAG(processorDescriptionGraph)

    val processors = dag.processors
    val stormConfig = Utils.readStormConfig()
    val userConfig = UserConfig.empty
      .withValue(TOPOLOGY, topology)
      .withValue(TASK_TO_COMPONENT, taskToComponent)
      .withString(STORM_CONFIG, JSONValue.toJSONString(stormConfig))
    val fieldGen = Gen.alphaStr

    val bolts = topology.get_bolts()
    forAll(fieldGen) { (field: String) =>
      processors foreach { case (pid, procDesc) =>
        val conf = procDesc.taskConf
        val cid = conf.getString(COMPONENT_ID).getOrElse(
          fail(s"component id not found for processor $pid")
        )
        val targets = StormUtil.getTargets(cid, topology)
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
            val fields = componentToStreamFields.get(scid).get(streamId.get_streamId())
            val values = List.fill(fields.size)(field)
            val stormTuple = StormTuple(values, scid, streamId.get_streamId(), 0, Map.empty[String, Int])
            stormProcessor.onNext(Message(stormTuple, System.currentTimeMillis()))
            if (targets.containsKey(streamId)) {
              verify(taskContext).output(anyObject())
            } else {
              verify(taskContext, times(0)).output(anyObject())
            }
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
