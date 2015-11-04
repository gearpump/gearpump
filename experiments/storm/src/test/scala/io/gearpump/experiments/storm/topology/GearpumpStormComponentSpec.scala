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
package io.gearpump.experiments.storm.topology

import akka.actor.ActorRef
import backtype.storm.spout.{SpoutOutputCollector, ISpout}
import backtype.storm.task.{OutputCollector, IBolt, GeneralTopologyContext, TopologyContext}
import java.util.{Map => JMap}
import backtype.storm.tuple.Tuple
import io.gearpump.{TimeStamp, Message}
import io.gearpump.experiments.storm.topology.GearpumpStormComponent.{GearpumpBolt, GearpumpSpout}
import io.gearpump.experiments.storm.util.StormOutputCollector
import io.gearpump.streaming.{MockUtil, DAG}
import io.gearpump.streaming.task.{TaskContext, StartTime, TaskId}
import org.mockito.Mockito._
import org.mockito.Matchers.{anyLong, anyObject, eq => mockitoEq}
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{PropSpec, Matchers}

class GearpumpStormComponentSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("GearpumpSpout lifecycle") {
    val config = mock[JMap[AnyRef, AnyRef]]
    val spout = mock[ISpout]
    val taskContext = MockUtil.mockTaskContext
    val appMaster = mock[ActorRef]
    when(taskContext.appMaster).thenReturn(appMaster)
    val getDAG = mock[ActorRef => DAG]
    val dag = mock[DAG]
    when(getDAG(appMaster)).thenReturn(dag)
    val getTopologyContext = mock[(DAG, TaskId) => TopologyContext]
    val topologyContext = mock[TopologyContext]
    when(getTopologyContext(dag, taskContext.taskId)).thenReturn(topologyContext)
    val getOutputCollector = mock[(TaskContext, TopologyContext) => StormOutputCollector]
    val stormOutputCollector = mock[StormOutputCollector]
    when(getOutputCollector(taskContext, topologyContext)).thenReturn(stormOutputCollector)

    val gearpumpSpout = GearpumpSpout(config, spout, getDAG, getTopologyContext,
      getOutputCollector, taskContext)

    // start
    val startTime = mock[StartTime]
    gearpumpSpout.start(startTime)

    verify(spout).open(mockitoEq(config), mockitoEq(topologyContext), anyObject[SpoutOutputCollector])

    // next
    val message = mock[Message]
    gearpumpSpout.next(message)

    verify(stormOutputCollector).setTimestamp(anyLong())
    verify(spout).nextTuple()
  }

  property("GearpumpBolt lifecycle") {
    val timestampGen = Gen.chooseNum[Long](0L, 1000L)
    val freqGen = Gen.chooseNum[Long](1L, 100L)
    forAll(timestampGen, freqGen) { (timestamp: TimeStamp, freq: Long) =>
      val config = mock[JMap[AnyRef, AnyRef]]
      val bolt = mock[IBolt]
      val taskContext = MockUtil.mockTaskContext
      val appMaster = mock[ActorRef]
      when(taskContext.appMaster).thenReturn(appMaster)
      val getDAG = mock[ActorRef => DAG]
      val dag = mock[DAG]
      when(getDAG(appMaster)).thenReturn(dag)
      val getTopologyContext = mock[(DAG, TaskId) => TopologyContext]
      val topologyContext = mock[TopologyContext]
      when(getTopologyContext(dag, taskContext.taskId)).thenReturn(topologyContext)
      val getGeneralTopologyContext = mock[DAG => GeneralTopologyContext]
      val generalTopologyContext = mock[GeneralTopologyContext]
      when(getGeneralTopologyContext(dag)).thenReturn(generalTopologyContext)
      val getOutputCollector = mock[(TaskContext, TopologyContext) => StormOutputCollector]
      val stormOutputCollector = mock[StormOutputCollector]
      when(getOutputCollector(taskContext, topologyContext)).thenReturn(stormOutputCollector)
      val getTickTuple = mock[(GeneralTopologyContext, Long) => Tuple]
      val tickTuple = mock[Tuple]
      when(getTickTuple(mockitoEq(generalTopologyContext), anyLong())).thenReturn(tickTuple)
      val gearpumpBolt = GearpumpBolt(config, bolt, getDAG, getTopologyContext, getGeneralTopologyContext,
        getOutputCollector, getTickTuple, taskContext)

      // start
      val startTime = mock[StartTime]
      gearpumpBolt.start(startTime)

      verify(bolt).prepare(mockitoEq(config), mockitoEq(topologyContext), anyObject[OutputCollector])

      // next
      val gearpumpTuple = mock[GearpumpTuple]
      val tuple = mock[Tuple]
      when(gearpumpTuple.toTuple(generalTopologyContext)).thenReturn(tuple)
      val message = Message(gearpumpTuple, timestamp)
      gearpumpBolt.next(message)

      verify(stormOutputCollector).setTimestamp(timestamp)
      verify(bolt).execute(tuple)


      // tick
      gearpumpBolt.tick(freq)
      verify(bolt).execute(tickTuple)
    }
  }

}
