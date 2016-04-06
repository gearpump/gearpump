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
package io.gearpump.experiments.storm.topology

import java.util.{List => JList}
import scala.collection.JavaConverters._

import backtype.storm.task.GeneralTopologyContext
import backtype.storm.tuple.Fields
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

import io.gearpump.TimeStamp

class GearpumpTupleSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("GearpumpTuple should create Storm Tuple") {
    val tupleGen = for {
      values <- Gen.listOf[String](Gen.alphaStr).map(_.distinct.asJava.asInstanceOf[JList[AnyRef]])
      sourceTaskId <- Gen.chooseNum[Int](0, Int.MaxValue)
      sourceStreamId <- Gen.alphaStr
    } yield new GearpumpTuple(values, new Integer(sourceTaskId), sourceStreamId, null)

    forAll(tupleGen, Gen.alphaStr, Gen.chooseNum[Long](0, Long.MaxValue)) {
      (gearpumpTuple: GearpumpTuple, componentId: String, timestamp: TimeStamp) =>
        val topologyContext = mock[GeneralTopologyContext]
        val fields = new Fields(gearpumpTuple.values.asScala.map(_.asInstanceOf[String]): _*)
        when(topologyContext.getComponentId(gearpumpTuple.sourceTaskId)).thenReturn(componentId)
        when(topologyContext.getComponentOutputFields(
          componentId, gearpumpTuple.sourceStreamId)).thenReturn(fields)

        val tuple = gearpumpTuple.toTuple(topologyContext, timestamp)

        tuple shouldBe a[TimedTuple]
        val timedTuple = tuple.asInstanceOf[TimedTuple]
        timedTuple.getValues shouldBe gearpumpTuple.values
        timedTuple.getSourceTask shouldBe gearpumpTuple.sourceTaskId
        timedTuple.getSourceComponent shouldBe componentId
        timedTuple.getSourceStreamId shouldBe gearpumpTuple.sourceStreamId
        timedTuple.getMessageId shouldBe null
        timedTuple.getFields shouldBe fields
        timedTuple.timestamp shouldBe timestamp
    }
  }
}
