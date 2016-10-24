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

package org.apache.gearpump.experiments.storm.partitioner

import java.util.{List => JList}
import scala.collection.JavaConverters._

import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

import org.apache.gearpump.Message
import org.apache.gearpump.experiments.storm.topology.GearpumpTuple
import org.apache.gearpump.streaming.partitioner.Partitioner

class StormPartitionerSpec extends PropSpec with PropertyChecks with Matchers {

  property("StormPartitioner should get partitions directed by message and target") {
    val idGen = Gen.chooseNum[Int](0, Int.MaxValue)
    val componentsGen = Gen.listOf[String](Gen.alphaStr).map(_.distinct).suchThat(_.size > 1)
    val partitionsGen = Gen.listOf[Int](idGen).suchThat(_.nonEmpty).map(_.distinct.sorted.toArray)
    val tupleFactoryGen = for {
      values <- Gen.listOf[String](Gen.alphaStr).map(_.asJava.asInstanceOf[JList[AnyRef]])
      sourceTaskId <- idGen
      sourceStreamId <- Gen.alphaStr
    } yield (targetPartitions: Map[String, Array[Int]]) => {
      new GearpumpTuple(values, new Integer(sourceTaskId), sourceStreamId, targetPartitions)
    }

    forAll(tupleFactoryGen, idGen, componentsGen, partitionsGen) {
      (tupleFactory: Map[String, Array[Int]] => GearpumpTuple, id: Int,
        components: List[String], partitions: Array[Int]) => {
        val currentPartitionId = id
        val targetPartitions = components.init.map(c => (c, partitions)).toMap
        val tuple = tupleFactory(targetPartitions)
        targetPartitions.foreach {
          case (target, ps) => {
            val partitioner = new StormPartitioner(target)
            ps shouldBe partitioner.getPartitions(Message(tuple), ps.last + 1, currentPartitionId)
          }
        }
        val partitionNum = id
        val nonTarget = components.last
        val partitioner = new StormPartitioner(nonTarget)

        partitioner.getPartitions(Message(tuple), partitionNum,
          currentPartitionId) shouldBe List(Partitioner.UNKNOWN_PARTITION_ID)
      }
    }
  }
}
