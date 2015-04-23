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

package org.apache.gearpump.experiments.storm.producer

import backtype.storm.tuple.Tuple
import backtype.storm.utils.Utils
import java.util.{List => JList}
import org.apache.gearpump.Message
import org.apache.gearpump.streaming.MockUtil
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

class StormSpoutOutputCollectorSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("StormSpoutOutputCollector should output messages as list") {
    val valGen = Gen.oneOf(Gen.alphaStr, Gen.alphaChar, Gen.chooseNum[Int](0, 1000))
    val valuesGen = Gen.listOf[AnyRef](valGen)
    val pidGen = Gen.chooseNum[Int](0, 1000)

    forAll(valuesGen, pidGen) { (values: List[AnyRef], pid: Int) =>
      val taskContext = MockUtil.mockTaskContext
      val outputFn = (streamId: String, tuple: JList[AnyRef]) => {
        taskContext.output(Message(tuple))
      }
      val collector = new StormSpoutOutputCollector(outputFn)
      collector.emit(Utils.DEFAULT_STREAM_ID, values, null)
      verify(taskContext).output(MockUtil.argMatch[Message] { msg =>
        msg.msg.asInstanceOf[JList[AnyRef]].asScala == values
      })
    }
  }

}
