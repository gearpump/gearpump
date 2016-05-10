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

import java.util.{List => JList}
import scala.collection.JavaConverters._

import backtype.storm.generated.GlobalStreamId
import backtype.storm.grouping.CustomStreamGrouping
import backtype.storm.task.TopologyContext
import backtype.storm.tuple.Fields
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

import org.apache.gearpump.experiments.storm.util.GrouperSpec.Value

class GrouperSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  val taskIdGen = Gen.chooseNum[Int](0, 1000)
  val valuesGen = Gen.listOf[String](Gen.alphaStr)
    .map(_.asJava.asInstanceOf[JList[AnyRef]])
  val numTasksGen = Gen.chooseNum[Int](1, 1000)

  property("GlobalGrouper should always return partition 0") {
    forAll(taskIdGen, valuesGen) { (taskId: Int, values: JList[AnyRef]) =>
      val grouper = new GlobalGrouper
      grouper.getPartitions(taskId, values) shouldBe List(0)
    }
  }

  property("NoneGrouper should returns partition in the range [0, numTasks)") {
    forAll(taskIdGen, valuesGen, numTasksGen) {
      (taskId: Int, values: JList[AnyRef], numTasks: Int) =>
        val grouper = new NoneGrouper(numTasks)
        val partitions = grouper.getPartitions(taskId, values)
        partitions.size shouldBe 1
        partitions.head should (be >= 0 and be < numTasks)
    }
  }

  property("ShuffleGrouper should return partition in the range [0, numTasks)") {
    forAll(taskIdGen, valuesGen, numTasksGen) {
      (taskId: Int, values: JList[AnyRef], numTasks: Int) =>
        val grouper = new ShuffleGrouper(numTasks)
        val partitions = grouper.getPartitions(taskId, values)
        partitions.size shouldBe 1
        partitions.head should (be >= 0 and be < numTasks)
    }
  }

  property("FieldsGrouper should return partition according to fields") {
    forAll(taskIdGen, numTasksGen) {
      (taskId: Int, numTasks: Int) =>
        val values = 0.until(numTasks).map(i => new Value(i))
        val fields = values.map(_.toString)
        val outFields = new Fields(fields: _*)
        values.flatMap { v =>
          val groupFields = new Fields(v.toString)
          val grouper = new FieldsGrouper(outFields, groupFields, numTasks)
          grouper.getPartitions(taskId,
            values.toList.asJava.asInstanceOf[JList[AnyRef]])
        }.distinct.size shouldBe numTasks
    }
  }

  property("AllGrouper should return all partitions") {
    forAll(taskIdGen, numTasksGen, valuesGen) {
      (taskId: Int, numTasks: Int, values: JList[AnyRef]) =>
        val grouper = new AllGrouper(numTasks)
        val partitions = grouper.getPartitions(taskId, values)
        partitions.distinct.size shouldBe numTasks
        partitions.min shouldBe 0
        partitions.max shouldBe (numTasks - 1)
    }
  }

  property("CustomGrouper should return partitions specified by user") {
    val grouping = mock[CustomStreamGrouping]
    val grouper = new CustomGrouper(grouping)
    val topologyContext = mock[TopologyContext]
    val globalStreamId = mock[GlobalStreamId]
    val sourceTasks = mock[JList[Integer]]

    grouper.prepare(topologyContext, globalStreamId, sourceTasks)

    verify(grouping).prepare(topologyContext, globalStreamId, sourceTasks)

    forAll(taskIdGen, valuesGen, numTasksGen) {(taskId: Int, values: JList[AnyRef], taskNum: Int) =>
      0.until(taskNum).foreach { i =>
        when(grouping.chooseTasks(taskId, values)).thenReturn(List(new Integer(i)).asJava)
        grouper.getPartitions(taskId, values) shouldBe List(i)
      }
    }
  }
}

object GrouperSpec {
  class Value(val i: Int) extends AnyRef {

    override def toString: String = s"$i"

    override def hashCode(): Int = i

    override def equals(other: Any): Boolean = {
      if (other.isInstanceOf[Value]) {
        this.i == other.asInstanceOf[Value].i
      } else {
        false
      }
    }
  }
}
