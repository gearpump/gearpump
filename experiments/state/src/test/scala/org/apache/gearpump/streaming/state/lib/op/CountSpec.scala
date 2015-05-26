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

package org.apache.gearpump.streaming.state.lib.op

import org.apache.gearpump._
import org.apache.gearpump.streaming.state.api.State
import org.mockito.Matchers._
import org.mockito.{Matchers => MockitoMatchers}
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{PropSpec, Matchers}

class CountSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("Count update should call state.update") {
    val longGen = Gen.chooseNum[Long](0L, 1000L)
    val messageGen = for {
      t <- longGen
      l <- longGen
    } yield Message(l.toString, t)
    forAll(messageGen) { (message: Message) =>
      val count = new Count
      val state = mock[State[Long]]

      count.update(state, message)
      verify(state).update(MockitoMatchers.eq(message.timestamp), anyLong(), anyObject[(Long, Long) => Long]())
    }
  }

}
