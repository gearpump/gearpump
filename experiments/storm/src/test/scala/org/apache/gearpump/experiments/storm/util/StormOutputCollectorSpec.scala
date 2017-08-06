/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.experiments.storm.util

import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConverters._
import backtype.storm.generated.Grouping
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}
import org.apache.gearpump.{Message, Time}
import org.apache.gearpump.Time.MilliSeconds
import org.apache.gearpump.experiments.storm.topology.GearpumpTuple
import org.apache.gearpump.streaming.MockUtil

class StormOutputCollectorSpec
  extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  private val stormTaskId = 0
  private val streamIdGen = Gen.alphaStr
  private val valuesGen = Gen.listOf[String](Gen.alphaStr).map(_.asJava.asInstanceOf[JList[AnyRef]])
  private val timestampGen = Gen.chooseNum[Long](0L, 1000L)

  property("StormOutputCollector emits tuple values into a stream") {
    forAll(timestampGen, streamIdGen, valuesGen) {
      (timestamp: MilliSeconds, streamId: String, values: JList[AnyRef]) =>
        val targets = mock[JMap[String, JMap[String, Grouping]]]
        val taskToComponent = mock[JMap[Integer, String]]
        val getTargetPartitionsFn = mock[(String, JList[AnyRef]) =>
          (Map[String, Array[Int]], JList[Integer])]
        val targetPartitions = mock[Map[String, Array[Int]]]
        val targetStormTaskIds = mock[JList[Integer]]
        when(getTargetPartitionsFn(streamId, values)).thenReturn((targetPartitions,
          targetStormTaskIds))
        val taskContext = MockUtil.mockTaskContext
        val stormOutputCollector = new StormOutputCollector(stormTaskId, taskToComponent,
          targets, getTargetPartitionsFn, taskContext, Time.MIN_TIME_MILLIS)

        when(targets.containsKey(streamId)).thenReturn(false)
        stormOutputCollector.emit(streamId, values) shouldBe StormOutputCollector.EMPTY_LIST
        verify(taskContext, times(0)).output(anyObject[Message])

        when(targets.containsKey(streamId)).thenReturn(true)
        stormOutputCollector.setTimestamp(timestamp)
        stormOutputCollector.emit(streamId, values) shouldBe targetStormTaskIds
        verify(taskContext, times(1)).output(MockUtil.argMatch[Message]({
          message: Message =>
            val expected = new GearpumpTuple(values, stormTaskId, streamId, targetPartitions)
            message.value == expected && message.timestamp.toEpochMilli == timestamp
        }))
    }
  }

  property("StormOutputCollector emit direct to a task") {
    val idGen = Gen.chooseNum[Int](0, 1000)
    val targetGen = Gen.alphaStr
    forAll(idGen, targetGen, timestampGen, streamIdGen, valuesGen) {
      (id: Int, target: String, timestamp: Long, streamId: String, values: JList[AnyRef]) =>
        val targets = mock[JMap[String, JMap[String, Grouping]]]
        val taskToComponent = mock[JMap[Integer, String]]
        when(taskToComponent.get(id)).thenReturn(target)
        val getTargetPartitionsFn = mock[(String, JList[AnyRef]) =>
          (Map[String, Array[Int]], JList[Integer])]
        val targetPartitions = mock[Map[String, Array[Int]]]
        val targetStormTaskIds = mock[JList[Integer]]
        when(getTargetPartitionsFn(streamId, values)).thenReturn((targetPartitions,
          targetStormTaskIds))
        val taskContext = MockUtil.mockTaskContext
        val stormOutputCollector = new StormOutputCollector(stormTaskId, taskToComponent,
          targets, getTargetPartitionsFn, taskContext, Time.MIN_TIME_MILLIS)

        when(targets.containsKey(streamId)).thenReturn(false)
        verify(taskContext, times(0)).output(anyObject[Message])

        when(targets.containsKey(streamId)).thenReturn(true)
        stormOutputCollector.setTimestamp(timestamp)
        stormOutputCollector.emitDirect(id, streamId, values)
        val partitions = Array(StormUtil.stormTaskIdToGearpump(id).index)
        verify(taskContext, times(1)).output(MockUtil.argMatch[Message]({
          message: Message => {
            val expected = new GearpumpTuple(values, stormTaskId, streamId,
            Map(target -> partitions))

            val result = message.value == expected && message.timestamp.toEpochMilli == timestamp
            result
          }
        }))
    }
  }
}
