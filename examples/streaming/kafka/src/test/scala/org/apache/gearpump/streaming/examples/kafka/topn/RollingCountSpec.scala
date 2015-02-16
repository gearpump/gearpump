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

package org.apache.gearpump.streaming.examples.kafka.topn

import org.apache.gearpump.streaming.MockUtil._
import org.apache.gearpump.{Message, TimeStamp}
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.MockUtil
import org.mockito.ArgumentMatcher
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

class RollingCountSpec extends FlatSpec with Matchers with MockitoSugar {
  val emitFrequencyMS = 1000
  val windowLengthMS = 10000

  val objCountGen = for {
    obj <- Gen.alphaStr
    count <- Gen.choose[Long](1, emitFrequencyMS)
  } yield (obj, count)

  val objCountMapGen = Gen.listOf[(String, TimeStamp)](objCountGen).map(_.toMap) suchThat (_.size > 0)

  val userConf = UserConfig.empty.
    withInt(Config.EMIT_FREQUENCY_MS, emitFrequencyMS).
    withInt(Config.WINDOW_LENGTH_MS, windowLengthMS)

  it should "output the object counts rolling with time" in {

    objCountMapGen.map { objCountMap =>
      val context = MockUtil.mockTaskContext

      val rollingCount = new RollingCount(context, UserConfig.empty)

      for (time <- 1 to emitFrequencyMS) {
        objCountMap.foreach { case (obj, count) =>
          if (count >= time) {
            rollingCount.onNext(Message(obj, time))
          }
        }
      }
      rollingCount.onNext(Message("fire", emitFrequencyMS + 1))

      verify(context,  times(objCountMap.size)).output(argMatch[Message]{ kv =>
        val (word, count) = kv.msg.asInstanceOf[(String, Long)]
        objCountMap.get(word) == Option(count)
      })
    }
  }
}
