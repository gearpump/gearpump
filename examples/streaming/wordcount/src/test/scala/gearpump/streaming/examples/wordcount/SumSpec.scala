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
package gearpump.streaming.examples.wordcount

import gearpump.Message
import gearpump.streaming.MockUtil
import gearpump.streaming.task.StartTime
import gearpump.cluster.UserConfig
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfter, Matchers, PropSpec}

class SumSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfter {
  val stringGenerator = Gen.alphaStr

  var wordcount = 0

  property("Sum should calculate the frequency of the word correctly"){

    val taskContext = MockUtil.mockTaskContext

    val conf = UserConfig.empty

    val sum = new Sum(taskContext, conf)

    sum.onStart(StartTime(0))

    forAll(stringGenerator) { txt =>
      wordcount += 1
      sum.onNext(Message(txt))
    }
    val all = sum.map.foldLeft(0L) { (total, kv) =>
      val (_, num) = kv
      total + num
    }
    assert(sum.wordCount == all && sum.wordCount == wordcount)

    sum.reportWordCount()
  }
}
