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
package io.gearpump.streaming.examples.kafka.wordcount

import scala.collection.mutable

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalacheck.Gen
import org.scalatest.{FlatSpec, Matchers}

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.MockUtil
import io.gearpump.streaming.task.StartTime

class SumSpec extends FlatSpec with Matchers {

  it should "sum should calculate the frequency of the word correctly" in {

    val stringGenerator = Gen.alphaStr
    val expectedWordCountMap: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()

    val taskContext = MockUtil.mockTaskContext

    val sum = new Sum(taskContext, UserConfig.empty)
    sum.onStart(StartTime(0))
    val str = "once two two three three three"

    var totalWordCount = 0
    stringGenerator.map { word =>
      totalWordCount += 1
      expectedWordCountMap.put(word, expectedWordCountMap.getOrElse(word, 0L) + 1)
      sum.onNext(Message(word))
    }
    verify(taskContext, times(totalWordCount)).output(anyObject[Message])

    expectedWordCountMap.foreach { wordCount =>
      val (word, count) = wordCount
      assert(count == sum.wordcount.get(word).get)
    }
  }
}
