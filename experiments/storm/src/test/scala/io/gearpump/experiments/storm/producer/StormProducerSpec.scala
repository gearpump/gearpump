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

// TODO: fix this spec
/*class StormProducerSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {
  import StormConstants._

  property("StormProducer should work") {
    implicit val system = ActorSystem("test",  TestUtil.DEFAULT_CONFIG)
    val topology = TopologyUtil.getTestTopology
    val graphBuilder = GraphBuilder
    val processorGraph = graphBuilder.build(topology, null)


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
        .withString(STORM_CONFIG, JSONValue.toJSONString(stormConfig))

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
}*/
