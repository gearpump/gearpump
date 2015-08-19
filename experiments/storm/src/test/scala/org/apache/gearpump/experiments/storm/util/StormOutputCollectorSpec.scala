/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.experiments.storm.util

import backtype.storm.utils.Utils
import org.apache.gearpump.{Message, TimeStamp}
import org.apache.gearpump.streaming.MockUtil
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

import scala.collection.JavaConversions._

class StormOutputCollectorSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("StormOutputCollector should output messages as list") {
    val valGen = Gen.oneOf(Gen.alphaStr, Gen.alphaChar, Gen.chooseNum[Int](0, 1000))
    val valuesGen = Gen.listOf[AnyRef](valGen)
    val timestampGen = Gen.chooseNum[Long](0L, Long.MaxValue)

    forAll(valuesGen, timestampGen) { (values: List[AnyRef], timestamp: TimeStamp) =>
      val context = MockUtil.mockTaskContext
      val streamId = Utils.DEFAULT_STREAM_ID
      val pid = 0
      val componentId = "component"
      val collector = new StormOutputCollector(context, pid, componentId)
      collector.setTimestamp(timestamp)
      collector.emit(streamId, values)

      val stormTuple = StormTuple(values, pid, componentId, streamId)
      verify(context).output(Message(stormTuple, timestamp))
    }
  }

}
