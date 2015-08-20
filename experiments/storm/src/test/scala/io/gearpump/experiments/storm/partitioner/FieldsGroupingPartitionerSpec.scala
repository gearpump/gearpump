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

package io.gearpump.experiments.storm.partitioner

import backtype.storm.tuple.Fields
import io.gearpump.experiments.storm.util.StormTuple
import io.gearpump.Message
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

import scala.collection.JavaConversions._

class FieldsGroupingPartitionerSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("FieldsGroupingPartitioner should get partition based on grouping fields' hashcode") {
    val outFieldsGen = Gen.listOf[String](Gen.alphaStr.suchThat(_.length > 0)).map(_.toSet.toList) suchThat (_.size > 0)
    val partitionNumGen = Gen.chooseNum[Int](1, 1000)
    val sourceTaskIdGen = Gen.chooseNum[Int](0, 1000)

    forAll(outFieldsGen, partitionNumGen, sourceTaskIdGen) { (outFields: List[String], partitionNum: Int, sourceTaskId: Int) =>
      val stormTuple = mock[StormTuple]
      when(stormTuple.tuple).thenReturn(outFields)
      1.to(outFields.length).foreach { num =>
        val groupingFields = outFields.take(num)
        val partitioner = new FieldsGroupingPartitioner(new Fields(outFields), new Fields(groupingFields))
        val actualPartition = partitioner.getPartition(Message(stormTuple), partitionNum)
        actualPartition should (be >= 0 and be < partitionNum)
      }

    }
  }
}
