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
package org.apache.gearpump.streaming.dsl.task

import java.time.Instant

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.dsl.window.api.CountWindow
import org.apache.gearpump.streaming.dsl.window.impl.{GroupAlsoByWindow, WindowRunner}
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class CountTriggerTaskSpec extends PropSpec with PropertyChecks
  with Matchers with MockitoSugar {

  property("CountTriggerTask should trigger output by number of messages in a window") {

    implicit val system = MockUtil.system

    val numGen = Gen.chooseNum[Int](1, 1000)

    forAll(numGen, numGen) { (windowSize: Int, msgNum: Int) =>

      val groupBy = mock[GroupAlsoByWindow[Any, Any]]
      val window = CountWindow.apply(windowSize)
      when(groupBy.window).thenReturn(window)
      val windowRunner = mock[WindowRunner]
      val userConfig = UserConfig.empty

      val task = new CountTriggerTask[Any, Any](groupBy, windowRunner,
        MockUtil.mockTaskContext, userConfig)
      val message = mock[Message]

      for (i <- 1 to msgNum) {
        task.onNext(message)
      }
      verify(windowRunner, times(msgNum)).process(message)
      verify(windowRunner, times(msgNum / windowSize)).trigger(Instant.ofEpochMilli(windowSize))
    }
  }
}
