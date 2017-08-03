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

package org.apache.gearpump.experiments.storm.producer

import backtype.storm.spout.ISpout
import backtype.storm.utils.Utils
import org.apache.gearpump.experiments.storm.util.StormOutputCollector
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

import scala.collection.JavaConverters._

class StormSpoutOutputCollectorSpec
  extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("StormSpoutOutputCollector should call StormOutputCollector") {
    val valGen = Gen.oneOf(Gen.alphaStr, Gen.alphaChar, Gen.chooseNum[Int](0, 1000))
    val valuesGen = Gen.listOf[AnyRef](valGen)

    forAll(valuesGen) { (values: List[AnyRef]) =>
      val collector = mock[StormOutputCollector]
      val spout = mock[ISpout]
      val streamId = Utils.DEFAULT_STREAM_ID
      val spoutCollector = new StormSpoutOutputCollector(collector, spout, false)
      spoutCollector.emit(streamId, values.asJava, null)
      verify(collector).emit(streamId, values.asJava)
    }
  }
}
