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

package io.gearpump.experiments.storm.processor

import java.util.{List => JList}
import scala.collection.JavaConverters._

import backtype.storm.tuple.Tuple
import backtype.storm.utils.Utils
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

import io.gearpump.experiments.storm.util.StormOutputCollector

class StormBoltOutputCollectorSpec
  extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("StormBoltOutputCollector should call StormOutputCollector") {
    val valGen = Gen.oneOf(Gen.alphaStr, Gen.alphaChar, Gen.chooseNum[Int](0, 1000))
    val valuesGen = Gen.listOf[AnyRef](valGen)

    forAll(valuesGen) { (values: List[AnyRef]) =>
      val collector = mock[StormOutputCollector]
      val boltCollector = new StormBoltOutputCollector(collector)
      val streamId = Utils.DEFAULT_STREAM_ID
      boltCollector.emit(streamId, null, values.asJava)
      verify(collector).emit(streamId, values.asJava)
    }
  }

  property("StormBoltOutputCollector should throw on fail") {
    val collector = mock[StormOutputCollector]
    val tuple = mock[Tuple]
    val boltCollector = new StormBoltOutputCollector(collector)
    an[Exception] should be thrownBy boltCollector.fail(tuple)
  }
}
