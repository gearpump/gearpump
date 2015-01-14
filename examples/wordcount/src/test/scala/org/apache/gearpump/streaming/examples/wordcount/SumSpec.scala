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
package org.apache.gearpump.streaming.examples.wordcount

import akka.actor.ActorSystem
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.{UserConfig, TestUtil}
import org.apache.gearpump.streaming.StreamingTestUtil
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

class SumSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter {
  val stringGenerator = Gen.alphaStr
  val system1 = ActorSystem("SumSpec", TestUtil.DEFAULT_CONFIG)
  val system2 = ActorSystem("Reporter", TestUtil.DEFAULT_CONFIG)
  val (sum, echo) = StreamingTestUtil.createEchoForTaskActor(classOf[Sum].getName, UserConfig.empty, system1, system2)
  val sumActor = sum.underlying.actor.asInstanceOf[Sum]
  var wordcount = 0

  property("Sum should calculate the frequency of the word correctly"){
    forAll(stringGenerator) { txt =>
      wordcount += 1
      sum.tell(Message(txt), sum)
    }
    val all = sumActor.map.foldLeft(0L) { (total, kv) =>
      val (_, num) = kv
      total + num
    }
    assert(sumActor.wordCount == all && sumActor.wordCount == wordcount)
  }

  after {
    system1.shutdown()
    system2.shutdown()
  }
}
