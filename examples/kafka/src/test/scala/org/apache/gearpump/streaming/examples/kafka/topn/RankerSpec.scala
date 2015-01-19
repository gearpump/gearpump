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

import akka.actor.ActorSystem
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.{UserConfig, TestUtil}
import org.apache.gearpump.streaming.StreamingTestUtil
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

class RankerSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter {
  val emitFrequencyMS = 1000
  val windowLengthMS = 10000
  val topn = 10

  val objCountGen = for {
    obj <- Gen.alphaStr
    count <- Gen.choose[Long](1, emitFrequencyMS * 10)
  } yield (obj, count)

  val userConf = UserConfig.empty.
    withInt(Config.EMIT_FREQUENCY_MS, emitFrequencyMS).
    withInt(Config.WINDOW_LENGTH_MS, windowLengthMS).
    withInt(Config.TOPN, topn)

  val system1 = ActorSystem("Ranker", TestUtil.DEFAULT_CONFIG)
  val system2 = ActorSystem("Reporter", TestUtil.DEFAULT_CONFIG)

  property("Ranker should output the right topN"){
    val (ranker, echo) = StreamingTestUtil.createEchoForTaskActor(classOf[Ranker].getName, userConf, system1, system2)
    var timeStamp = 1
    val rankings = new Rankings[String]
    //Set minSuccessful to trigger the output action of Ranker
    implicit val generatorDrivenConfig = PropertyCheckConfig(minSuccessful = emitFrequencyMS + 1)

    forAll(objCountGen) { kv =>
      val (obj, count) = kv
      rankings.update(obj, count)
      ranker.tell(Message(kv, timeStamp), ranker)
      timeStamp += 1
    }

    rankings.getTopN(topn).foreach { result =>
      assert(result == echo.receiveN(1).head.asInstanceOf[Message].msg)
    }
  }

  after {
    system1.shutdown()
    system2.shutdown()
  }
}
