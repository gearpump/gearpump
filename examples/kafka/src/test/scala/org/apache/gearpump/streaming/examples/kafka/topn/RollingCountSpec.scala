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
import org.apache.gearpump.{TimeStamp, Message}
import org.apache.gearpump.cluster.{TestUtil, UserConfig}
import org.apache.gearpump.streaming.StreamingTestUtil
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

class RollingCountSpec extends PropSpec with PropertyChecks with Matchers {
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

  property("RollingCount should output the object counts rolling with time"){

    forAll(objCountMapGen) { (objCountMap: Map[String, Long]) =>
      val system1 = ActorSystem("RollingCount", TestUtil.DEFAULT_CONFIG)
      val system2 = ActorSystem("Reporter", TestUtil.DEFAULT_CONFIG)
      val (rollingCount, echo) = StreamingTestUtil.createEchoForTaskActor(classOf[RollingCount].getName, userConf, system1, system2)

      for (time <- 1 to emitFrequencyMS) {
        objCountMap.foreach { case (obj, count) =>
          if (count >= time) {
            rollingCount.tell(Message(obj, time), rollingCount)
          }
        }
      }
      rollingCount.tell(Message("fire", emitFrequencyMS + 1), rollingCount)

      val received = echo.receiveN(objCountMap.size).asInstanceOf[Vector[Message]]
        .map(_.msg.asInstanceOf[(String, Long)]).toMap

      objCountMap shouldBe received

      system1.shutdown()
      system2.shutdown()
    }
  }
}
